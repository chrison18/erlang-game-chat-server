-module(chat_client_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("chat_protocol.hrl").

name_length_boundary_test() ->
    Utf8Character = <<16#4F60/utf8>>,
    MaxName = binary:copy(
        Utf8Character, 16#FFFF div byte_size(Utf8Character)),
    ?assertEqual(16#FFFF, byte_size(MaxName)),
    LoginPacket = chat_client_protocol:encode_login(MaxName, <<"password">>),
    <<?PROTO_LOGIN_REQUEST:16, 16#FFFF:16, _/binary>> = LoginPacket,
    PrivatePacket =
        chat_client_protocol:encode_private_send(MaxName, <<"content">>),
    <<?PROTO_PRIVATE_SEND_REQUEST:16, 16#FFFF:16, _/binary>> = PrivatePacket,

    TooLongName = <<MaxName/binary, $a>>,
    ?assertEqual(
        {error, role_name_too_long},
        chat_client_protocol:encode_login(TooLongName, <<"password">>)),
    ?assertEqual(
        {error, target_role_name_too_long},
        chat_client_protocol:encode_private_send(
            TooLongName, <<"content">>)),
    ?assertEqual(16#10000, byte_size(TooLongName)).

channel_push_batch_decodes_in_order_test() ->
    First = chat_server_protocol:encode_channel_push(
        2, 101, <<"alice">>, <<"first">>),
    Second = chat_server_protocol:encode_channel_push(
        2, 102, <<"bob">>, <<"second">>),
    Packet = chat_server_protocol:encode_channel_push_batch([First, Second]),
    ?assertEqual(
        {ok, {channel_push_batch,
              [#{channel_id => 2,
                 sender_role_id => 101,
                 sender_role_name => <<"alice">>,
                 content => <<"first">>},
               #{channel_id => 2,
                 sender_role_id => 102,
                 sender_role_name => <<"bob">>,
                 content => <<"second">>}]}},
        chat_client_protocol:decode_packet(Packet)),
    ?assertEqual(
        {ok, {channel_push_batch, []}},
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_channel_push_batch([]))).

channel_push_batch_rejects_invalid_boundaries_test() ->
    ChannelPush = chat_server_protocol:encode_channel_push(
        2, 101, <<"alice">>, <<"hello">>),
    Length = byte_size(ChannelPush),
    WrongType = chat_server_protocol:encode_channel_join_result({ok, 2}),

    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_CHANNEL_PUSH_BATCH:16, 2:16,
              Length:32, ChannelPush/binary>>)),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_CHANNEL_PUSH_BATCH:16, 1:16,
              (Length + 1):32, ChannelPush/binary>>)),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_CHANNEL_PUSH_BATCH:16, 1:16,
              (Length - 1):32, ChannelPush/binary>>)),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_CHANNEL_PUSH_BATCH:16, 1:16,
              (byte_size(WrongType)):32, WrongType/binary>>)),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_CHANNEL_PUSH_BATCH:16, 1:16,
              Length:32, ChannelPush/binary, 0:8>>)).

aoi_event_roundtrip_and_validation_test() ->
    ?assertEqual(
        {ok, {aoi_event, #{event => enter, role_id => 101}}},
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_aoi_event(enter, 101))),
    ?assertEqual(
        {ok, {aoi_event, #{event => leave, role_id => 102}}},
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_aoi_event(leave, 102))),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_AOI_EVENT_PUSH:16, 3:8, 101:32>>)),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_AOI_EVENT_PUSH:16, ?AOI_EVENT_ENTER:8, 101:32, 0:8>>)),
    ?assertEqual(
        {error, invalid_packet},
        chat_client_protocol:decode_packet(
            <<?PROTO_AOI_EVENT_PUSH:16, ?AOI_EVENT_ENTER:8, 101:24>>)).
