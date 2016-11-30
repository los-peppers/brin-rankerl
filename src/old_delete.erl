-module(tmp_mapreduce).


%% API
-export([doMap/4,test/0]).
-include("../include/brin.hrl").


%%%%%%%%%%%%%%%%%%%%
%%%%%MAP%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%

test()->
  doMap({123,456},[0.25,0.25,0.25,0.25],0,4).

readMtx(_FilePath) ->
%%    io:format("~p",[FilePath]),
  [
    #node{source=1,degree=3,destinations=[2,3,4]},
    #node{source=2,degree=2,destinations=[1,4]},
    #node{source=3,degree=1,destinations=[1]},
    #node{source=4,degree=2,destinations=[2,3]}
  ].

doMap({ChunkId,ColId},VectorChunk,Beta,K) ->
  FilePath = parseFilePath(ChunkId,ColId),
  MatrixChunk = readMtx(FilePath),
  doNTimes(VectorChunk,{MatrixChunk,Beta,K},50).

doNTimes(Vector,_Params,0)->Vector;
doNTimes(Vector,Params = {MatrixChunk,Beta,K},N)->
  io:format("N ~p~n", [N]),
  NewVector = operateVector(MatrixChunk,Vector,Beta,K),
  io:format("~p~n", [NewVector]),
  doNTimes(NewVector,Params,N-1).


parseFilePath(_X,_Y) -> "sdfasdf".


hasVal([E|_],Num) when E==Num ->
  true;
hasVal([E|Rest],Num) when E =/= Num ->
  hasVal(Rest,Num);
hasVal([],_) ->
  false.

getOrZero(#node{degree=Dg,destinations=Dest},I) ->
  case hasVal(Dest,I) of
    true -> Dg;
    false -> 0
  end.

%TODO: see if K shoud be the matrix size.
operateVector(MatrixChunk,VectorChunk,_Beta,_K) ->
  lists:map(fun(I)->
    Row = [getOrZero(Node,I) || Node <- MatrixChunk],
    Zipped = lists:zip(VectorChunk,Row),
    Mapped = lists:map(fun({Vi,Dg})->
      case Dg of
        0 -> 0;
        _ -> Vi/Dg %TODO: enter full formula here.
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
% doReduce({RowId,ChunkStepList}) ->
%   Summed = lists:sum(ChunkStepList),
%   {RowId,Summed}.