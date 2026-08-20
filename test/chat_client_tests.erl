-module(chat_client_tests).

-include_lib("eunit/include/eunit.hrl").

unsupported_call_replies_without_stopping_test() ->
    State = #{socket => test_socket},
    ?assertEqual(
        {reply, {error, {unsupported_call, unknown_request}}, State},
        chat_client:handle_call(unknown_request, {self(), make_ref()}, State)).

aoi_event_is_handled_as_observer_push_test() ->
    State = #{socket => test_socket,
              mode => observer,
              feedback => false,
              observer_received => 2},
    Packet = chat_server_protocol:encode_aoi_event(enter, 101),
    ?assertEqual(
        {noreply, State#{observer_received := 3}},
        chat_client:handle_info({tcp, test_socket, Packet}, State)).

aoi_move_cycle_schedules_ten_unique_moves_test() ->
    rand:seed(exsss, {101, 202, 303}),
    State = #{mode => aoi_move, action_seq => 1},
    {noreply, ScheduledState} =
        chat_client:handle_info(start_aoi_move, State),
    CycleRef = maps:get(aoi_move_cycle_ref, ScheduledState),
    timer:sleep(1005),
    Messages = element(2, process_info(self(), messages)),
    MoveMessages = [
        Message
     || {aoi_move_action, Ref, _Offset, _ExpiresAt} = Message <- Messages,
        Ref =:= CycleRef
    ],
    Offsets = lists:sort([
        Offset
     || {aoi_move_action, _Ref, Offset, _ExpiresAt} <- MoveMessages
    ]),
    ?assertEqual(10, length(MoveMessages)),
    ?assertEqual(10, length(lists:usort(Offsets))),
    ?assert(lists:all(fun(Offset) -> Offset >= 0 andalso Offset < 1000 end,
                     Offsets)),
    ?assert(lists:member({aoi_move_cycle, CycleRef}, Messages)),
    ?assertEqual([], [Message || Message <- Messages,
                                not is_aoi_move_message(Message, CycleRef)]),
    flush_messages(Messages).

aoi_move_cycle_regenerates_offsets_test() ->
    FirstOffsets = scheduled_offsets({11, 22, 33}),
    SecondOffsets = scheduled_offsets({44, 55, 66}),
    ?assertNotEqual(FirstOffsets, SecondOffsets).

aoi_move_stop_does_not_schedule_another_cycle_test() ->
    CycleRef = make_ref(),
    State = #{mode => aoi_move,
              action_seq => 1,
              aoi_move_cycle_ref => CycleRef},
    {reply, ok, StoppedState} =
        chat_client:handle_call(stop_aoi_move, {self(), make_ref()}, State),
    ?assertEqual(aoi_move_stopped, maps:get(mode, StoppedState)),
    ?assertEqual(
        {noreply, StoppedState},
        chat_client:handle_info({aoi_move_cycle, CycleRef}, StoppedState)),
    ?assertEqual({messages, []}, process_info(self(), messages)).

expired_aoi_move_is_not_caught_up_test() ->
    CycleRef = make_ref(),
    State = #{mode => aoi_move,
              action_seq => 1,
              aoi_move_cycle_ref => CycleRef},
    ExpiredAt = erlang:monotonic_time(millisecond),
    ?assertEqual(
        {noreply, State},
        chat_client:handle_info(
            {aoi_move_action, CycleRef, 10, ExpiredAt}, State)).

scheduled_offsets(Seed) ->
    rand:seed(exsss, Seed),
    State = #{mode => aoi_move, action_seq => 1},
    {noreply, ScheduledState} =
        chat_client:handle_info(start_aoi_move, State),
    CycleRef = maps:get(aoi_move_cycle_ref, ScheduledState),
    timer:sleep(1005),
    Messages = element(2, process_info(self(), messages)),
    Offsets = lists:sort([
        Offset
     || {aoi_move_action, Ref, Offset, _ExpiresAt} <- Messages,
        Ref =:= CycleRef
    ]),
    flush_messages(Messages),
    Offsets.

is_aoi_move_message({aoi_move_action, CycleRef, _Offset, _ExpiresAt},
                    CycleRef) ->
    true;
is_aoi_move_message({aoi_move_cycle, CycleRef}, CycleRef) ->
    true;
is_aoi_move_message(_Message, _CycleRef) ->
    false.

flush_messages(Messages) ->
    lists:foreach(
        fun(Message) ->
            receive Message -> ok
            after 0 -> ok
            end
        end,
        Messages).
