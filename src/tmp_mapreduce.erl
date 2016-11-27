%%%-------------------------------------------------------------------
%%% @author aleph
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Nov 2016 10:41 PM
%%%-------------------------------------------------------------------
-module(tmp_mapreduce).
-author("aleph").

-behaviour(gen_server).

%% API
-export([]).

-record(node,{
  source,
  degree,
  destinations = []
}).

%%%%%%%%%%%%%%%%%%%%
%%%%%MAP%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%
doMap({ChunkId,ColId},VectorChunk,Beta,K) ->
  FilePath = parseFilePath(ChunkId,ColId),
  MatrixChunk = readMatrixChunk(FilePath),
  NewVectorChunk = operateVector(MatrixChunk,VectorChunk,Beta,K),
  NewVectorChunk. %[{RowId,Summed}...]

parseFilePath(_X,_Y) -> "sdfasdf".

readMatrixChunk(_FilePath) ->
  [
    node#{source=1,degree=3,destinations=[2,3,4]},
    node#{source=2,degree=3,destinations=[1,4]},
    node#{source=3,degree=3,destinations=[3]},
    node#{source=4,degree=3,destinations=[2,4]}
  ].

operateVector(MatrixChunk,_VectorChunk,_Beta,_K) ->
  io:format("Matrix", [MatrixChunk]).
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