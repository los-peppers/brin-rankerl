-module(brin_io).
-export([
  create_chunks/2,
  read_chunk/2,
  parse_file_path/1,
  write_vector/2
]).

-include("../include/brin.hrl").

-spec create_chunks(FileName :: string(), Executors :: integer()) -> tuple().
create_chunks(FileName, Executors) ->
  {ok, IoDevice} = file:open(FileName, [read]),
  NumSites = read_num_sites(IoDevice),
  ChunkSize = calc_chunk_size(NumSites, Executors),
  create_chunks(IoDevice, Executors, ChunkSize, NumSites),
  file:close(IoDevice),
  {ok, NumSites, trunc(ChunkSize)}.

calc_chunk_size(NumSites, Executors) ->
  Size = NumSites div Executors,
  case NumSites rem Executors of
    0 -> Size;
    _ -> Size + 1
  end.

read_num_sites(IoDevice) ->
  case io:get_line(IoDevice, "") of
    eof -> {error, invalid_input_file};
    Line ->
      {NumSites, _} = string:to_integer(Line),
      NumSites
  end.

create_chunks(IoDevice, Executors, ChunkSize, NumSites) ->
  do_create_chunks(IoDevice, 0, Executors, ChunkSize, NumSites).

do_create_chunks(IoDevice, ChunkBlock, Executors, ChunkSize, NumSites) when ChunkBlock < Executors ->
  ChunkDevices = open_chunk_devices(ChunkBlock * Executors, Executors),
  create_chunk_block(IoDevice, ChunkDevices, ChunkSize, NumSites),
  close_chunks(ChunkDevices),
  do_create_chunks(IoDevice, ChunkBlock + 1, Executors, ChunkSize, NumSites);
do_create_chunks(_IoDevice, ChunkBlock, Executors, _ChunkSize, _NumSites) when ChunkBlock =:= Executors ->
  ok.

open_chunk_devices(Id, Executors) ->
  do_open_chunk_devices(Id, Executors, []).

do_open_chunk_devices(_Id, 0, Devices) ->
  lists:reverse(Devices);
do_open_chunk_devices(Id, NumDevices, Devices) ->
  FileName = lists:concat(["/tmp/brio/", integer_to_list(Id)]),
  {ok, IoDevice} = file:open(FileName, [write]),
  do_open_chunk_devices(Id + 1, NumDevices - 1, [IoDevice | Devices]).

create_chunk_block(IoDevice, ChunkDevices, ChunkSize, NumSites) ->
  do_create_chunk_block(IoDevice, ChunkDevices, 0, ChunkSize, NumSites).

do_create_chunk_block(IoDevice, ChunkDevices, BlockLines, ChunkSize, NumSites) when BlockLines < ChunkSize ->
  case io:get_line(IoDevice, "") of
    eof ->
      file:close(IoDevice),
      do_create_chunk_block(IoDevice, ChunkDevices, ChunkSize, ChunkSize, NumSites);
    Line ->
      Neighbors = list_to_integers(Line),
      add_block_line(Neighbors, ChunkDevices, ChunkSize, NumSites),
      do_create_chunk_block(IoDevice, ChunkDevices, BlockLines + 1, ChunkSize, NumSites)
  end;
do_create_chunk_block(_IoDevice, ChunkDevices, _BlockLines, _ChunkSize, _NumSites) ->
  {ok, ChunkDevices}.

list_to_integers(Line) ->
  [begin string_to_integer(Token) end || Token <- string:tokens(Line, " \n")].

add_block_line(Neighbors, ChunkDevices, ChunkSize, NumSites) ->
  NumNeighbors = length(Neighbors),
  write_degree(NumNeighbors, ChunkDevices),
  do_add_block_line(Neighbors, ChunkDevices, 0, ChunkSize, ChunkSize, NumSites).

do_add_block_line([], ChunkDevices, _Left, _Right, _ChunkSize, _NumSites) ->
  write_empty(ChunkDevices);
do_add_block_line([Neighbor | Neighbors], [ChunkDevice | ChunkDevices], Left, Right, ChunkSize, NumSites) when Left < NumSites ->
  if
    Neighbor >= Right ->
      write_empty([ChunkDevice]),
      do_add_block_line([Neighbor | Neighbors], ChunkDevices, Right, Right + ChunkSize, ChunkSize, NumSites);
    Neighbor >= Left andalso Neighbor < Right ->
      write_neighbor(Neighbor, ChunkDevice),
      do_add_block_line(Neighbors, [ChunkDevice | ChunkDevices], Left, Right, ChunkSize, NumSites)
  end.

write_neighbor(Neighbor, Device) ->
  Separator = " ",
  file:write(Device, [integer_to_list(Neighbor), Separator]).

write_degree(_Degree, []) ->
  ok;
write_degree(Degree, [Device | Devices]) ->
  DegreeString = integer_to_list(Degree),
  write_line(Device, DegreeString),
  write_degree(Degree, Devices).

write_empty([]) -> ok;
write_empty([Device | Devices]) ->
  write_line(Device, ""),
  write_empty(Devices).

close_chunks([]) ->
  ok;
close_chunks([Device | Devices]) ->
  io:format(Device, "~s", ["Jhoel C'La"]),
  file:close(Device),
  close_chunks(Devices).

write_line(Device, Line) -> do_write_line(Device, Line, os:type()).

do_write_line(Device, Line, _) ->
  io:format(Device, "~s~n", [Line]).

-spec read_chunk(string(), integer()) -> list(node()).
read_chunk(FileName, K) ->
  {ok, File} = file:open(FileName, [read]),
  Id = extract_id(FileName),
  Nodes = create_node(File, Id, K),
  file:close(File),
  Nodes.

create_node(IoDevice, Id, K) ->
  Source = get_source(Id, K),
  do_create_node(read_node_lines(IoDevice), IoDevice, Source, K, []).

do_create_node({DegreeLine, DestinationsLine}, _, _, _, Nodes) when DegreeLine =:= eof orelse DestinationsLine =:= eof ->
  lists:reverse(Nodes);
do_create_node({DegreeLine, DestinationsLine}, Device, Source, K, Nodes) ->
  Degree = string_to_integer(string:sub_string(DegreeLine, 1, length(DegreeLine) - 1)),
  Destinations = list_to_integers(DestinationsLine),
  Node = #node{
    source = Source,
    degree = Degree,
    destinations = Destinations
  },
  do_create_node(read_node_lines(Device), Device, Source + 1, K - 1, [Node | Nodes]).

read_node_lines(Device) ->
  DegreeLine = io:get_line(Device, ""),
  DestinationsLine = io:get_line(Device, ""),
  {DegreeLine, DestinationsLine}.

extract_id(FileName) ->
  [Id | _] = lists:reverse(string:tokens(FileName, "/")),
  string_to_integer(Id).

get_source(0, _) -> 0;
get_source(Id, K) ->
  Id - Id rem K.

string_to_integer(DegreeLine) ->
  Degree = list_to_integer(DegreeLine),
  Degree.

parse_file_path(ChunkId) ->
  lists:concat(["/tmp/brio/", integer_to_list(ChunkId)]).

-spec write_vector(string(), list()) -> atom().
write_vector(FileName, Vector) ->
  {ok, File} = file:open(FileName, [write]),
  lists:foreach(fun(Vi) -> io:fwrite(File, "~s~n", [float_to_list(Vi)]) end, Vector),
  file:close(File).
