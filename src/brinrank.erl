-module(brinrank).

-export([
  calc/6
]).

calc(InputFileName, Beta, K, Iterations, Nodes, OutputFileName) ->
  NumChunks = trunc(math:pow(K, 2)),
  {ok, N, ChunkSize} = brin_io:create_chunks(InputFileName, length(Nodes) * K),
  SchedulerExecutors = lists:flatten([lists:duplicate(K, Node) || Node <- Nodes]),
  Scheduler = brin_scheduler:init(SchedulerExecutors, NumChunks, brin_ops, handle_map),
  VectorChunk = lists:map(fun(_) -> 1 / N end, lists:seq(1, N)),
  ResultVector = calc_aux(Scheduler, VectorChunk, ChunkSize, Beta, N, length(SchedulerExecutors), NumChunks, Iterations),
  brin_io:write_vector(OutputFileName, ResultVector).

calc_aux(_Scheduler, _VectorChunk, _ChunkSize, _Beta, _N, _Executors, _NumChunks, 0) ->
  _VectorChunk;
calc_aux(Scheduler, VectorChunk, ChunkSize, Beta, N, Executors, NumChunks, Iterations) ->
  MapResult = run_maps(Scheduler, VectorChunk, ChunkSize, Beta, N, Executors, NumChunks),
  NewVector = collect_results(MapResult, Beta, N),
  calc_aux(Scheduler, NewVector, ChunkSize, Beta, N, Executors, NumChunks, Iterations - 1).

run_maps(Scheduler, Vector, ChunkSize, Beta, N, NumExecutors, NumChunks) ->
  {HeadVector, TailVector} = lists:split(length(Vector) - ChunkSize, Vector),
  run_maps_aux(Scheduler, TailVector, HeadVector, ChunkSize, Beta, N, NumExecutors, NumExecutors, NumChunks),
  receive {ok, Result} ->
    io:format("Map Result: ~n~p~n", [Result]),
    Result
  end.


run_maps_aux(_Scheduler, _ChunkVector, [], _ChunkSize, _Beta, _N, _NumExecutors, 0, 0) -> ok;
run_maps_aux(Scheduler, _ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, 0, ChunkId) ->
  Index = max(0, length(Vector) - ChunkSize),
  {HeadVector, TailVector} = lists:split(Index, Vector),
  run_maps_aux(Scheduler, TailVector, HeadVector, ChunkSize, Beta, N, NumExecutors, NumExecutors, ChunkId);
run_maps_aux(Scheduler, ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, ExecutorCnt, ChunkId) ->
  Scheduler ! {schedule, {map, ChunkId - 1, NumExecutors, ChunkVector, Beta, N}},
  run_maps_aux(Scheduler, ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, ExecutorCnt - 1, ChunkId - 1).

collect_results(TupleResults, Beta, N) ->
  Map = collect_results(TupleResults, maps:new()),
  SortedTuples = lists:sort(fun({Key1, _}, {Key2, _}) -> Key1 =< Key2 end, maps:to_list(Map)),
  lists:map(
    fun({_Key, Value}) ->
      Value + (1 - Beta) / N
    end, SortedTuples).

collect_results([], Map) -> Map;
collect_results([{Key, Val} | Rest], Map) ->
  NewMap = maps:update_with(Key, fun(MapVal) -> MapVal + Val end, Val, Map),
  collect_results(Rest, NewMap).

