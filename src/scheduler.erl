%%%=============================================================================
%%% @doc Task scheduler used to orchestrate maps and reduces across nodes.
%%% @private
%%%
%%% @end
%%%=============================================================================

-module(brin_scheduler).
-export([init/4]).

%% @doc Creates a new scheduler process linked to the caller.
-spec init(list(node()), integer(), module(), atom()) -> pid().
init(Workers, TaskCnt, TaskModule, TaskFun) ->
    MasterPid = self(),
    spawn_link(fun() ->
        process_flag(trap_exit, true),
        event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, dict:new(), [])
    end).

%% @doc Runs a continues loop managing tasks, nodes and responses.
-spec event_loop(pid(), module(), atom(), list(node()), integer(), dict:dict(), list()) -> none().
event_loop(MasterPid, _, _, _, 0, _, Results) -> MasterPid ! {ok, Results};
event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, Tasks, Results) ->
    RunngingTasks = dict:size(Tasks),
    io:format("DEBUG: Receiving~n"),
    receive
        % Schedule task when workers are available
        {schedule, Task} when Workers =/= [] ->
            io:format("DEBUG: Scheduling tastk~n"),
            [Worker|NewWorkers] = Workers,
            case net_adm:ping(Worker) of
                pong ->
                    % Run task on worker node
                    TaskPid =  run_task(Worker, TaskModule, TaskFun, [self(), Task]),
                    NewTasks = dict:append(TaskPid, {Worker, Task, []}, Tasks);
                pang ->
                    % Worker node down
                    self() ! {schedule, Task},
                    NewTasks = Tasks
            end,
            event_loop(MasterPid, TaskModule, TaskFun, NewWorkers, TaskCnt, NewTasks, Results);
        % Scheduling task when there are no workers available or busy
        {schedule, _} when Workers =:= [], RunngingTasks =:= 0 -> exit(nowokers);
        % Receive a list of partial responses from worker
        {emit, TaskPid, PartialResults} when is_list(PartialResults) ->
            io:format("DEBUG: emit~n"),
            UpdateFun = fun ([{Worker, Task, TaskResults}]) ->
                [{Worker, Task, lists:append(PartialResults, TaskResults)}]
            end,
            NewTasks = dict:update(TaskPid, UpdateFun, Tasks),
            event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, NewTasks, Results);
        % Receive a partial response from worker
        {emit, TaskPid, PartialResult} ->
            io:format("DEBUG: emit~n"),
            UpdateFun = fun ([{Worker, Task, TaskResults}]) ->
                [{Worker, Task, [PartialResult|TaskResults]}]
            end,
            NewTasks = dict:update(TaskPid, UpdateFun, Tasks),
            event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, NewTasks, Results);
        % Handle the normal exit of a worker
        {'EXIT', TaskPid, normal} when TaskPid =/= self() ->
            io:format("DEBUG: Task ~p finished successfully~n", [TaskPid]),
            [{Worker, _, PartialResults}] = dict:fetch(TaskPid, Tasks),
            NewTasks = dict:erase(TaskPid, Tasks),
            NewWorkers = [Worker|Workers],
            NewTaskCnt = TaskCnt - 1,
            NewResults = lists:append(PartialResults, Results),
            event_loop(MasterPid, TaskModule, TaskFun, NewWorkers, NewTaskCnt, NewTasks, NewResults);
        % Handle the unexpected exit of a worker
        {'EXIT', TaskPid, Reason} when TaskPid =/= self() ->
            io:format("DEBUG: Task ~p finished unexpectedly. Reason: ~p~n", [TaskPid, Reason]),
            [{Worker, Task, _}] = dict:fetch(TaskPid, Tasks),
            NewTasks = dict:erase(TaskPid, Tasks),
            NewWorkers = [Worker|Workers],
            self() ! {schedule, Task},
            event_loop(MasterPid, TaskModule, TaskFun, NewWorkers, TaskCnt, NewTasks, Results)
    end.

%% @doc Runs a function on a node. Retuns the ID of the linked process that
%% handles the call. Errors on the remote call cause an exit.
-spec run_task(node(), module(), atom(), list(term())) -> pid().
run_task(Node, Module, Fun, Params) ->
    spawn_link(fun() ->
            case rpc:call(Node, Module, Fun, [self()|Params]) of
                {badrpc, Reason} -> exit(Reason);
                _ -> ok
            end
        end
    ).
