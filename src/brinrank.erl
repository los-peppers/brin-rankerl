-module(brinrank).

%% Chúpenmela
-export([
  calc/6
]).

%%mainThread() -> mainThread("../data/entrada.txt", 1).

calc(InputFileName, Beta, K, Iteraciones, Nodes, OutputFileName) ->
  NumChunks = trunc(math:pow(K, 2)),
  {ok, N, ChunkSize} = brin_io:create_chunks(InputFileName, length(Nodes) * K),
  SchedulerExecutors = lists:flatten([lists:duplicate(K, Node) || Node <- Nodes]),
  Scheduler = brin_scheduler:init(SchedulerExecutors, NumChunks, brin_ops, handle_map),
  VectorChunk = lists:map(fun(_) -> 1 / N end, lists:seq(1, N)),
  MapResult = run_maps(Scheduler, VectorChunk, ChunkSize, Beta, N, length(SchedulerExecutors), NumChunks),
  MapResult.

%%run_maps(Scheduler, Vector, ChunkSize, Beta, N, NumExecutors, NumChunks) ->
%%  run_maps_aux(Scheduler, [], Vector, ChunkSize, Beta, N, NumExecutors, NumChunks),
%%  receive {ok, Result} -> Result end.
%%
%%run_maps_aux(_, _, _, _, _, _, _, -1) -> ok;
%%run_maps_aux(Scheduler, _, Vector, ChunkSize, Beta, N, ExecutorCnt, ChunkID) when ChunkID rem ExecutorCnt =:= 0 ->
%%  {Head, Tail} = lists:split(ExecutorCnt, Vector),
%%  run_maps_aux(Scheduler, Head, Tail, ChunkSize, Beta, N, ExecutorCnt, ChunkID);
%%run_maps_aux(Scheduler, VectorChunk, Vector, ChunkSize, Beta, N, ExecutorCnt, ChunkID) ->
%%  Scheduler ! {schedule, {map, ChunkID, VectorChunk, Beta, N}},
%%  run_maps_aux(Scheduler, VectorChunk, Vector, ChunkSize, Beta, N, ExecutorCnt, ChunkID -1).

run_maps(_Scheduler, _Vector, _ChunkSize, _Beta, _N, 0, _NumChunks) ->
  receive {ok, Result} -> Result end;
run_maps(Scheduler, Vector, ChunkSize, Beta, N, NumExecutors, NumChunks) ->
  {HeadVector, TailVector} = lists:split(N - NumExecutors, Vector),
  run_maps_aux(Scheduler, TailVector, HeadVector, ChunkSize, Beta, N, NumExecutors, NumExecutors, NumChunks).

run_maps_aux(_Scheduler, _ChunkVector, [], _ChunkSize, _Beta, _N, _NumExecutors, 0, 0) -> ok;
run_maps_aux(Scheduler, _ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, 0, ChunkId) ->
  Index = max(0, length(Vector) - NumExecutors),
  {HeadVector, TailVector} = lists:split(Index, Vector),
  run_maps_aux(Scheduler, TailVector, HeadVector, ChunkSize, Beta, N, NumExecutors, NumExecutors, ChunkId);
run_maps_aux(Scheduler, ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, ExecutorCnt, ChunkId) ->
  Scheduler ! {schedule, {map, ChunkId - 1, NumExecutors, ChunkVector, Beta, N}},
  run_maps_aux(Scheduler, ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, ExecutorCnt - 1, ChunkId - 1).

%%mainThread(File, Executors) ->
%%  io:format("M: init~n"),
%%  Scheduler = brin_scheduler:init(['bottom@127.0.0.1', 'right@127.0.0.1'], 2, brin_ops, handle_map),
%%%    {ok, NumSites, NumberMapTasks, ChunkSize} = brin_io:create_chunks(File,Executors),
%%
%%%%    lists:map(fun(ChunkId)->
%%%%        %Generate map task.
%%%%        Scheduler ! {schedule, {map,{ChunkId,}}},
%%%%    end,lists:seq(1,NumberMapTasks)), %TODO finish
%%
%%  % Test
%%  io:format("Sending map"),
%%  Scheduler ! {schedule, {map, 1, 4, 0.8, 1, 4}}, % K = Executors*, N = NumSites
%%
%%  io:format("M: send first~n"),
%%%    Scheduler ! {schedule, {7000, 4}},
%%  io:format("M: send second~n"),
%%%    Scheduler ! {schedule, {7000, 6}}, io:format("M: wait~n"),
%%  receive
%%    {ok, Res} ->
%%      io:format("Results: ~p~n", [Res]),
%%  % Scheduler ! {schedule,{reduce,{RowId,[list of things]}}}
%%  end,
%%  io:format("done~n").
%%
%%
%%test(TaskPid, Dest, {Milis, Res}) ->
%%  Dest ! {emit, TaskPid, node()},
%%  io:format("Running on ~p~n", [node()]),
%%  timer:sleep(Milis),
%%  Dest ! {emit, TaskPid, Res}.
