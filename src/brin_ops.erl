-module(brin_ops).


%% API
-export([handle_map/3,doMap/5,test/0,doReduce/1]).

-record(node,{
  source,
  degree,
  destinations = []
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%RPC CALLBACKS%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_map(TaskPid, Dest, {map,ChunkId,ChunkSize,Beta,K,N}) ->
    io:format("called handle map"),
    VectorChunk = lists:map(fun(_)-> 1/N end,lists:seq(1,ChunkSize)), %TODO: not generate at once. maybe
    Dest ! {emit, TaskPid, node()},
%    io:format("Running on ~p~n", [node()]),
    Res = doMap(ChunkId,VectorChunk,Beta,K,N),
    Dest ! {emit, TaskPid, Res}.

%%%%%%%%%%%%%%%%%%%%
%%%%%MAP%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%

test()->
  doMap(0,[0.25,0.25,0.25,0.25],0.8,4,4).

doMap(ChunkId,VectorChunk,Beta,K,N) ->
  FilePath = brin_io:parse_file_path(ChunkId),
  io:format("FilePath: ~p~n",[FilePath]),
%%  MatrixChunk = brin_io:read_chunk(FilePath,1),
  MatrixChunk = readMtx(""),
  io:format("~p~n",[MatrixChunk]),
  doUntilConverged(VectorChunk,{MatrixChunk,Beta,K,N},1000,0.01).

readMtx(_FilePath) ->
%%    io:format("~p",[_FilePath]),
  [
    #node{source=0,degree=3,destinations=[1,2,3]},
    #node{source=1,degree=2,destinations=[0,3]},
    #node{source=2,degree=1,destinations=[0]},
    #node{source=3,degree=2,destinations=[1,2]}
  ].

doUntilConverged(Vector,{MatrixChunk,Beta,K,N},MaxIterations,Delta) when MaxIterations > 0 ->
    NewVector = operateVector(MatrixChunk,Vector,Beta,K,N),
    case isConverged(Vector,NewVector,Delta) of
        true ->
         NewVector;
        _ -> doUntilConverged(NewVector,{MatrixChunk,Beta,K,N},MaxIterations-1,Delta)
    end;
doUntilConverged(Vector,_Params,MaxIterations,_Delta) when MaxIterations =:= 0 ->
    Vector.

isConverged(Old,New,Delta) ->
    isConverged(lists:zip(Old,New),Delta).
isConverged([],_) ->
    true;
isConverged([{A,B}|Rest],Delta) when abs(A-B) < Delta ->
    isConverged(Rest,Delta);
isConverged([{A,B}|_],Delta) when abs(A-B) > Delta ->
    false.


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
  end,lists:seq(0,_K-1)).
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