-module(brin_run).

-export([mainThread/2,mainThread/0]).

mainThread()-> mainThread("entrada.txt",1).

mainThread(File,Executors) ->
    io:format("M: init~n"),
    Scheduler = brin_scheduler:init(['bottom@127.0.0.1', 'right@127.0.0.1'], 2, scheduler, test),
%    {ok, NumSites, NumberMapTasks, ChunkSize} = brin_io:create_chunks(File,Executors),

%    lists:map(fun(ChunkId)->
%        %Generate map task.
%         Scheduler ! {schedule, {map,{ChunkId,}}},
%    end,lists:seq(1,NumberMapTasks)), %TODO finish

    % Test
%    Scheduler ! {schedule,{map,ChunkId,ChunkSize,0.8,Executors,NumSites}}, % K = Executors*, N = NumSites

    io:format("M: send first~n"),
%    Scheduler ! {schedule, {7000, 4}},
    io:format("M: send second~n"),
%    Scheduler ! {schedule, {7000, 6}}, io:format("M: wait~n"),
    receive
        {ok, Res} ->
            io:format("Results: ~p~n", [Res])
    end,
    io:format("done~n").


test(TaskPid, Dest, {Milis, Res}) ->
    Dest ! {emit, TaskPid, node()},
    io:format("Running on ~p~n", [node()]),
    timer:sleep(Milis),
    Dest ! {emit, TaskPid, Res}.
