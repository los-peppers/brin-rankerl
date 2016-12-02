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
  Dest ! {emit, self(), Res};
handle_map(Dest, {reduce, {Key, ListVals}}) ->
  Res = doReduce({Key, ListVals}),
  io:format("Emiting: ~p~n", [Res]),
  Dest ! {emit, self(), Res}.

%%%%%%%%%%%%%%%%%%%%
%%%%%MAP%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%
test() ->
  doMap(0, [0.25, 0.25, 0.25, 0.25], 0.8, 4, 4).

doMap(ChunkId, VectorChunk, Beta, K, N) ->
  FilePath = brin_io:parse_file_path(ChunkId),
  io:format("Reading chunk at: ~s~n", [FilePath]),
  MatrixChunk = brin_io:read_chunk(FilePath, K),
%%  doUntilConverged(VectorChunk,{MatrixChunk,Beta,K,N},1000,0.01).
  operateVector(MatrixChunk, VectorChunk, Beta, N).

hasVal([E | _], Num) when E == Num ->
  true;
hasVal([E | Rest], Num) when E =/= Num ->
  hasVal(Rest, Num);
hasVal([], _) ->
  false.

getOrZero(Node = #node{source = Source, degree = Degree, destinations = Dest}, I) ->
  io:format("Node: ~p~n", [Node]),
  case hasVal(Dest, I) of
    true -> {Source, Degree};
    false -> {Source, 0}
  end.

%TODO: see if K shoud be the matrix size.
% Beta is a small probability of jumping to a random page,
% K is the size of the chunk, N is total number of nodes
operateVector(MatrixChunk, VectorChunk, Beta, N) ->
  lists:map(fun(I) ->
    Row = [{Source, _} | _] = [getOrZero(Node, I) || Node <- MatrixChunk],
    Zipped = lists:zip(VectorChunk, Row), %{VectorVal,RowVal}
    Mapped = lists:map(fun({Vi, {_, Dg}}) ->
      case Dg of
        0 -> 0;
        _ -> Beta * Vi / Dg + (1 - Beta) / N % Page 179.
      end
                       end, Zipped),
    {Source, lists:sum(Mapped)}
            end, lists:seq(0, length(VectorChunk)-1)).
%%  operateVector(MatrixChunk,VectorChunk,Beta,[]);
%%operateVector([Row|Rest],VectorChunk,Beta,Acc) ->
%Result = VECTOR * ROW WITH THAT RANDOM SHIT IN THE FORMULA
%operateVector(Rest,VectorChunk,Beta,[Result|Acc]);
%%  operateVector([],_VectorChunk,Beta,Acc) ->
%%lists:reverse(Acc).

%%%%%%%%%%%%%%%%%%%%
%%%%%REDUCE%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%
%Chunk step list is the result of partial sums for each of the row values
doReduce({RowId, ChunkStepList}) ->
  Summed = lists:sum(ChunkStepList),
  {RowId, Summed}.
