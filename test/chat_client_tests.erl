-module(chat_client_tests).

-include_lib("eunit/include/eunit.hrl").

unsupported_call_replies_without_stopping_test() ->
    State = #{socket => test_socket},
    ?assertEqual(
        {reply, {error, {unsupported_call, unknown_request}}, State},
        chat_client:handle_call(unknown_request, {self(), make_ref()}, State)).
