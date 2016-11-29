-module(brin_io).
-export([read_file/2]).

-spec read_file(FileName :: string(), Executors :: integer()) -> ok.
read_file(FileName, Executors) ->
  {ok, IoDevice} = file:open(FileName, [read]),
  NumSites = read_num_sites(IoDevice),
  ChunkSize = NumSites div Executors + 1,
  create_chunks(IoDevice, Executors, ChunkSize, NumSites),
  file:close(IoDevice).

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
  FileName = lists:concat(["/tmp/brio/", integer_to_list(Id), ".pr"]),
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
      Neighbors = line_to_list(Line),
      add_block_line(Neighbors, ChunkDevices, ChunkSize, NumSites),
      do_create_chunk_block(IoDevice, ChunkDevices, BlockLines + 1, ChunkSize, NumSites)
  end;
do_create_chunk_block(_IoDevice, ChunkDevices, _BlockLines, _ChunkSize, _NumSites) ->
  {ok, ChunkDevices}.

line_to_list(Line) ->
  [begin {Int, _} = string:to_integer(Token), Int end || Token <- string:tokens(Line, " \n")].

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
