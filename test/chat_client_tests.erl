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
