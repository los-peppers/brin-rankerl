-module(brin_ops).


%% API
-export([handle_map/3, doMap/5, test/0, doReduce/1]).

-record(node, {
  source,
  degree,
  destinations = []
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%RPC CALLBACKS%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_map(TaskPid, Dest, {map, ChunkId, K, Vector, Beta, N}) ->
  Res = doMap(ChunkId, Vector, Beta, K, N),
  io:format("Emiting: ~p~n", [Res]),
  Dest ! {emit, TaskPid, Res}.

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
  operateVector(ChunkId, matrixChunk, VectorChunk, Beta, K, N).

hasVal([E | _], Num) when E == Num ->
  true;
hasVal([E | Rest], Num) when E =/= Num ->
  hasVal(Rest, Num);
hasVal([], _) ->
  false.

getOrZero(#node{degree = _Dg, destinations = Dest}, I) ->
  case hasVal(Dest, I) of
    true -> _Dg;
    false -> 0
  end.

%TODO: see if K shoud be the matrix size.
% Beta is a small probability of jumping to a random page,
% K is the size of the chunk, N is total number of nodes
operateVector(ChunkId, MatrixChunk, VectorChunk, Beta, K, N) ->
  lists:map(fun(I) ->
    Row = [getOrZero(Node, I) || Node <- MatrixChunk],
    Zipped = lists:zip(VectorChunk, Row), %{VectorVal,RowVal}
    Mapped = lists:map(fun({Vi, Dg}) ->
      case Dg of
        0 -> 0;
        _ -> Beta * Vi / Dg + (1 - Beta) / N % Page 179.
      end
                       end, Zipped),
    {ChunkId * K + I, lists:sum(Mapped)}
            end, lists:seq(0, length(VectorChunk))).
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


%readMtx(_FilePath) ->
%%%    io:format("~p",[FilePath]),
%  [
%    #node{source=1,degree=3,destinations=[2,3,4]},
%    #node{source=2,degree=2,destinations=[1,4]},
%    #node{source=3,degree=1,destinations=[1]},
%    #node{source=4,degree=2,destinations=[2,3]}
%  ].

%doNTimes(Vector,_Params,0)->Vector;
%doNTimes(Vector,Params = {MatrixChunk,Beta,K,N},Iters)->
%  NewVector = operateVector(MatrixChunk,Vector,Beta,K,N),
%%  io:format("~p~n", [NewVector]),
%  doNTimes(NewVector,Params,Iters-1).