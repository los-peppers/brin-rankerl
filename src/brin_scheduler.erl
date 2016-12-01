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
  spawn_link(
    fun() ->
      process_flag(trap_exit, true),
      event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, dict:new(), [])
    end
  ).

%% @doc Runs a continues loop managing tasks, nodes and responses.
-spec event_loop(pid(), module(), atom(), list(node()), integer(), dict:dict(), list()) -> none().
event_loop(MasterPid, _, _, _, 0, _, Results) -> MasterPid ! {ok, Results};
event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, Tasks, Results) ->
  RunningTasks = dict:size(Tasks),
  receive
    % Schedule task when workers are available
    {schedule, Task} when Workers =/= [] ->
      [Worker|NewWorkers] = Workers,
      NewTasks = case net_adm:ping(Worker) of
        pong ->
          % Run task on worker node
          TaskPid = spawn_link(Worker, TaskModule, TaskFun, [self(), Task]),
          dict:append(TaskPid, {Worker, Task, []}, Tasks);
        pang ->
          % Worker node down
          self() ! {schedule, Task},
          Tasks
      end,
      event_loop(MasterPid, TaskModule, TaskFun, NewWorkers, TaskCnt, NewTasks, Results);
    % Scheduling task when there are no workers available or busy
    {schedule, _} when Workers =:= [], RunningTasks =:= 0 -> exit(nowokers);
    % Receive a list of partial responses from worker
    {emit, TaskPid, PartialResults} when is_list(PartialResults) ->
      UpdateFun = fun([{Worker, Task, TaskResults}]) ->
        [{Worker, Task, lists:append(PartialResults, TaskResults)}]
                  end,
      NewTasks = dict:update(TaskPid, UpdateFun, Tasks),
      event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, NewTasks, Results);
    % Receive a partial response from worker
    {emit, TaskPid, PartialResult} ->
      UpdateFun = fun([{Worker, Task, TaskResults}]) ->
        [{Worker, Task, [PartialResult|TaskResults]}]
      end,
      NewTasks = dict:update(TaskPid, UpdateFun, Tasks),
      event_loop(MasterPid, TaskModule, TaskFun, Workers, TaskCnt, NewTasks, Results);
    % Handle the normal exit of a worker
    {'EXIT', TaskPid, normal} when TaskPid =/= self() andalso TaskPid =/= MasterPid->
      [{Worker, _, PartialResults}] = dict:fetch(TaskPid, Tasks),
      NewTasks = dict:erase(TaskPid, Tasks),
      NewWorkers = [Worker|Workers],
      NewTaskCnt = TaskCnt - 1,
      NewResults = lists:append(PartialResults, Results),
      event_loop(MasterPid, TaskModule, TaskFun, NewWorkers, NewTaskCnt, NewTasks, NewResults);
    % Handle the unexpected exit of a worker
    {'EXIT', TaskPid, Reason} when TaskPid =/= self() andalso TaskPid =/= MasterPid ->
      [{Worker, Task, _}] = dict:fetch(TaskPid, Tasks),
      io:format("ERROR: Task ~p on worker ~p finished unexpectedly. Reason: ~p~n", [TaskPid, Worker, Reason]),
      NewTasks = dict:erase(TaskPid, Tasks),
      NewWorkers = [Worker|Workers],
      self() ! {schedule, Task},
      event_loop(MasterPid, TaskModule, TaskFun, NewWorkers, TaskCnt, NewTasks, Results)
  end.
