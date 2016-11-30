-module(brin_ops).


%% API
-export([doMap/5,test/0,doReduce/1]).

-record(node,{
  source,
  degree,
  destinations = []
}).

%%%%%%%%%%%%%%%%%%%%
%%%%%MAP%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%

test()->
  doMap({123,456},[0.25,0.25,0.25,0.25],0.8,4,4).

readMtx(_FilePath) ->
%%    io:format("~p",[FilePath]),
  [
    #node{source=1,degree=3,destinations=[2,3,4]},
    #node{source=2,degree=2,destinations=[1,4]},
    #node{source=3,degree=1,destinations=[1]},
    #node{source=4,degree=2,destinations=[2,3]}
  ].

doMap({ChunkId,ColId},VectorChunk,Beta,K,N) ->
  FilePath = parseFilePath(ChunkId,ColId),
  MatrixChunk = readMtx(FilePath),
  doNTimes(VectorChunk,{MatrixChunk,Beta,K,N},5000).

doNTimes(Vector,_Params,0)->Vector;
doNTimes(Vector,Params = {MatrixChunk,Beta,K,N},Iters)->
  NewVector = operateVector(MatrixChunk,Vector,Beta,K,N),
%  io:format("~p~n", [NewVector]),
  doNTimes(NewVector,Params,Iters-1).


parseFilePath(_X,_Y) -> "sdfasdf".


hasVal([E|_],Num) when E==Num ->
  true;
hasVal([E|Rest],Num) when E =/= Num ->
  hasVal(Rest,Num);
hasVal([],_) ->
  false.

getOrZero(#node{degree=_Dg,destinations=Dest},I) ->
  case hasVal(Dest,I) of
    true -> _Dg;
    false -> 0
  end.

%TODO: see if K shoud be the matrix size.
% Beta is a small probability of jumping to a random page,
% K is the size of the chunk, N is total number of nodes
operateVector(MatrixChunk,VectorChunk,Beta,_K,N) ->
  lists:map(fun(I)->
    Row = [getOrZero(Node,I) || Node <- MatrixChunk],
    Zipped = lists:zip(VectorChunk,Row), %{VectorVal,RowVal}
    io:format("~p~n",[Row]),
    Mapped = lists:map(fun({Vi,Dg})->
      io:format("~p / ~p ~n",[Vi,Dg]),
      case Dg of
        0 -> 0;
        _ -> Beta*Vi/Dg + (1-Beta)/N % Page 179.
      end
    end,Zipped),
    lists:sum(Mapped)%Assuming elements are in order.
  end,lists:seq(1,_K)).
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
 doReduce({RowId,ChunkStepList}) ->
   Summed = lists:sum(ChunkStepList),
   {RowId,Summed}.