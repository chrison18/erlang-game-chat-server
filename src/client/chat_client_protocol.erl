-module(chat_client_protocol).

-include("chat_protocol.hrl").

-export([encode_login/2, decode_packet/1]).

encode_login(RoleName, Password) ->
    NameLength = byte_size(RoleName),
    <<?PROTO_LOGIN_REQUEST:16, NameLength:16,
      RoleName/binary, Password/binary>>.

decode_packet(<<?PROTO_LOGIN_RESULT:16, ?RESULT_SUCCESS:8,
                RoleId:32, ChannelCount:16, ChannelData/binary>>)
  when byte_size(ChannelData) =:= ChannelCount * 4 ->
    ChannelIds = [ChannelId || <<ChannelId:32>> <= ChannelData],
    {ok, {login_result, {ok, RoleId, ChannelIds}}};
decode_packet(<<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_INVALID_LOGIN:8>>) ->
    {ok, {login_result, {error, invalid_login}}};
decode_packet(<<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_ALREADY_ONLINE:8>>) ->
    {ok, {login_result, {error, already_online}}};
decode_packet(<<?PROTO_LOGIN_RESULT:16, _Data/binary>>) ->
    {error, invalid_packet};
decode_packet(<<?PROTO_ERROR:16, RequestProtoId:16, ErrorCode:8>>) ->
    {ok, {server_error, RequestProtoId, decode_error(ErrorCode)}};
decode_packet(<<ProtoId:16, _Data/binary>>) ->
    {error, {unexpected_proto, ProtoId}};
decode_packet(_Packet) ->
    {error, invalid_packet}.

decode_error(?ERROR_NOT_LOGGED_IN) -> not_logged_in;
decode_error(?ERROR_INVALID_PACKET) -> invalid_packet;
decode_error(?ERROR_UNKNOWN_PROTO) -> unknown_proto;
decode_error(ErrorCode) -> {unknown_error, ErrorCode}.
