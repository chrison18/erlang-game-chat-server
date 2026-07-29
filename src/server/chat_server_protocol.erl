-module(chat_server_protocol).

-include("chat_protocol.hrl").

-export([decode_request/1, encode_login_result/1, encode_error/2]).

decode_request(<<?PROTO_LOGIN_REQUEST:16, NameLength:16, Data/binary>>) ->
    case Data of
        <<RoleName:NameLength/binary, Password/binary>> ->
            {ok, {login, RoleName, Password}};
        _ ->
            {error, {invalid_packet, ?PROTO_LOGIN_REQUEST}}
    end;
decode_request(<<?PROTO_LOGIN_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_LOGIN_REQUEST}};
decode_request(<<ProtoId:16, Data/binary>>) ->
    case is_request_proto(ProtoId) of
        true -> {ok, {request, ProtoId, Data}};
        false -> {error, {unknown_proto, ProtoId}}
    end;
decode_request(_Packet) ->
    {error, {invalid_packet, 0}}.

encode_login_result({ok, RoleId, ChannelIds}) ->
    ChannelCount = length(ChannelIds),
    ChannelData = << <<ChannelId:32>> || ChannelId <- ChannelIds >>,
    <<?PROTO_LOGIN_RESULT:16, ?RESULT_SUCCESS:8, RoleId:32,
      ChannelCount:16, ChannelData/binary>>;
encode_login_result({error, invalid_login}) ->
    <<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_INVALID_LOGIN:8>>;
encode_login_result({error, already_online}) ->
    <<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_ALREADY_ONLINE:8>>.

encode_error(RequestProtoId, not_logged_in) ->
    <<?PROTO_ERROR:16, RequestProtoId:16, ?ERROR_NOT_LOGGED_IN:8>>;
encode_error(RequestProtoId, invalid_packet) ->
    <<?PROTO_ERROR:16, RequestProtoId:16, ?ERROR_INVALID_PACKET:8>>;
encode_error(RequestProtoId, unknown_proto) ->
    <<?PROTO_ERROR:16, RequestProtoId:16, ?ERROR_UNKNOWN_PROTO:8>>.

is_request_proto(?PROTO_CHANNEL_LIST_REQUEST) -> true;
is_request_proto(?PROTO_CHANNEL_JOIN_REQUEST) -> true;
is_request_proto(?PROTO_CHANNEL_LEAVE_REQUEST) -> true;
is_request_proto(?PROTO_CHANNEL_SEND_REQUEST) -> true;
is_request_proto(?PROTO_PRIVATE_SEND_REQUEST) -> true;
is_request_proto(_ProtoId) -> false.
