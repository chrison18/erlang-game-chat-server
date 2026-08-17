-module(chat_server_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("chat_protocol.hrl").

known_packets_decode_to_business_items_test() ->
    ?assertEqual(
        {ok, list_channels},
        chat_server_protocol:decode_request(
            chat_client_protocol:encode_channel_list())),
    ?assertEqual(
        {ok, {join_channel, 7}},
        chat_server_protocol:decode_request(
            chat_client_protocol:encode_channel_join(7))),
    ?assertEqual(
        {ok, {login, <<"alice">>, <<"secret">>}},
        chat_server_protocol:decode_request(
            chat_client_protocol:encode_login(<<"alice">>, <<"secret">>))).

known_fixed_structure_short_and_trailing_bytes_test() ->
    ?assertEqual(
        {error, {invalid_packet, ?PROTO_CHANNEL_JOIN_REQUEST}},
        chat_server_protocol:decode_request(
            <<?PROTO_CHANNEL_JOIN_REQUEST:16, 1:24>>)),
    ?assertEqual(
        {error, {invalid_packet, ?PROTO_CHANNEL_JOIN_REQUEST}},
        chat_server_protocol:decode_request(
            <<?PROTO_CHANNEL_JOIN_REQUEST:16, 1:32, 0:8>>)),
    ?assertEqual(
        {error, {invalid_packet, ?PROTO_CHANNEL_LIST_REQUEST}},
        chat_server_protocol:decode_request(
            <<?PROTO_CHANNEL_LIST_REQUEST:16, 0:8>>)).

unknown_proto_with_or_without_payload_test() ->
    ?assertEqual(
        {error, {unknown_proto, 65535}},
        chat_server_protocol:decode_request(<<65535:16>>)),
    ?assertEqual(
        {error, {unknown_proto, 65534}},
        chat_server_protocol:decode_request(<<65534:16, 1, 2, 3>>)).

packets_shorter_than_proto_id_test() ->
    ?assertEqual(
        {error, {invalid_packet, 0}},
        chat_server_protocol:decode_request(<<>>)),
    ?assertEqual(
        {error, {invalid_packet, 0}},
        chat_server_protocol:decode_request(<<1:8>>)).

login_declared_name_longer_than_remaining_data_test() ->
    ?assertEqual(
        {error, {invalid_packet, ?PROTO_LOGIN_REQUEST}},
        chat_server_protocol:decode_request(
            <<?PROTO_LOGIN_REQUEST:16, 3:16, <<"ab">>/binary>>)).
