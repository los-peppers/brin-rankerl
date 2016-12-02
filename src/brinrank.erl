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
  GroupedByKey = collect_results(MapResult),
  ReduceResult = run_reduce(Scheduler, GroupedByKey),
  SortedResultingVector = lists:sort(ReduceResult, fun({KeyA, _}, {KeyB, _}) -> KeyA < KeyB end),
  calc_aux(Scheduler, SortedResultingVector, ChunkSize, Beta, N, Executors, NumChunks, Iterations - 1).

run_maps(Scheduler, Vector, ChunkSize, Beta, N, NumExecutors, NumChunks) ->
  {HeadVector, TailVector} = lists:split(N - (NumExecutors * ChunkSize), Vector),
  run_maps_aux(Scheduler, TailVector, HeadVector, ChunkSize, Beta, N, NumExecutors, NumExecutors, NumChunks),
  receive {ok, Result} ->
    io:format("Map Result: ~n~p~n", [Result]),
    Result
  end.

run_maps_aux(_Scheduler, _ChunkVector, [], _ChunkSize, _Beta, _N, _NumExecutors, 0, 0) -> ok;
run_maps_aux(Scheduler, _ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, 0, ChunkId) ->
  Index = max(0, length(Vector) - NumExecutors),
  {HeadVector, TailVector} = lists:split(Index, Vector),
  run_maps_aux(Scheduler, TailVector, HeadVector, ChunkSize, Beta, N, NumExecutors, NumExecutors, ChunkId);
run_maps_aux(Scheduler, ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, ExecutorCnt, ChunkId) ->
  Scheduler ! {schedule, {map, ChunkId - 1, NumExecutors, ChunkVector, Beta, N}},
  run_maps_aux(Scheduler, ChunkVector, Vector, ChunkSize, Beta, N, NumExecutors, ExecutorCnt - 1, ChunkId - 1).

collect_results(TupleResults) ->
  collect_results(TupleResults, maps:new()).
collect_results([], Map) ->
  Map;
collect_results([{Key, Val} | Rest], Map) ->
  case maps:find(Key, Map) of
    {ok, List} ->
      NewList = [Val | List],
      collect_results(Rest, maps:update(Key, NewList, Map));
    _ ->
      NewList = [Val],
      collect_results(Rest, maps:put(Key, NewList, Map))
  end.

run_reduce(Scheduler, GroupedByKey) when is_map(GroupedByKey) ->
  lists:foreach(
    fun({Key, ListVals}) ->
      Scheduler ! {schedule, {reduce, {Key, ListVals}}}
    end, maps:to_list(GroupedByKey)),
  receive {ok, Result} ->
    io:format("Reduce Result: ~n~p~n", [Result]),
    Result
  end.

