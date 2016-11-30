%%%%%%%%%%%%%%%%%%%%% TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export([
    test/3,
    testLeft/0
]).


test(TaskPid, Dest, {Milis, Res}) ->
    Dest ! {emit, TaskPid, node()},
    io:format("Running on ~p~n", [node()]),
    timer:sleep(Milis),
    Dest ! {emit, TaskPid, Res}.

testLeft() ->
    io:format("M: init~n"),
    Scheduler = brin_scheduler:init(['bottom@127.0.0.1', 'right@127.0.0.1'], 2, scheduler, test),
    io:format("M: send first~n"),
    Scheduler ! {schedule, {7000, 4}},
    io:format("M: send second~n"),
    Scheduler ! {schedule, {7000, 6}}, io:format("M: wait~n"),
    receive
        {ok, Res} ->
            io:format("Results: ~p~n", [Res])
    end,
    io:format("done~n").
%%%%%%%%%%%%%%%%%%%%% TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%