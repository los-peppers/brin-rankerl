-module(brin_ops).


%% API
-export([handle_map/2]).

-record(node, {
  source,
  degree,
  destinations = []
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%RPC CALLBACKS%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_map(Dest, {map, ChunkId, K, Vector, Beta, N}) ->
  Res = doMap(ChunkId, Vector, Beta, K, N),
  io:format("Emiting: ~p~n", [Res]),
  Dest ! {emit, self(), Res}.

%%%%%%%%%%%%%%%%%%%%
%%%%%MAP%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%
doMap(ChunkId, VectorChunk, Beta, K, N) ->
  FilePath = brin_io:parse_file_path(ChunkId),
  io:format("Reading chunk at: ~s~n", [FilePath]),
  MatrixChunk = brin_io:read_chunk(FilePath, K),
  Start = ChunkId rem K * length(VectorChunk),
  operateVector(Start, MatrixChunk, VectorChunk, Beta, N).

hasVal([E | _], Num) when E == Num ->
  true;
hasVal([E | Rest], Num) when E =/= Num ->
  hasVal(Rest, Num);
hasVal([], _) ->
  false.

getOrZero(Node = #node{degree = Degree, destinations = Dest}, I) ->
  io:format("Node: ~p~n", [Node]),
  case hasVal(Dest, I) of
    true -> Degree;
    false -> 0
  end.

% Beta is a small probability of jumping to a random page,
% K is the size of the chunk, N is total number of nodes
operateVector(Start, MatrixChunk = [Node | _], VectorChunk, Beta, N) ->
  lists:map(fun(I) ->
    Row = [getOrZero(Node, I) || Node <- MatrixChunk],
    io:format("Row: ~p~n", [Row]),
    Zipped = lists:zip(VectorChunk, Row),
    Mapped = lists:map(fun({Vi, Dg}) ->
      case Dg of
        0 -> 0;
        _ -> (Beta * Vi) / Dg
      end
                       end, Zipped),
    {I, lists:sum(Mapped)}
            end, lists:seq(Start, Start + length(VectorChunk) - 1)).

%%%%%%%%%%%%%%%%%%%%
%%%%%REDUCE%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%
%Chunk step list is the result of partial sums for each of the row values
doReduce({RowId, ChunkStepList}) ->
  Summed = lists:sum(ChunkStepList),
  {RowId, Summed}.
