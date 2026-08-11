-module(chat_server_protocol).

-include("chat_protocol.hrl").

-export([decode_request/1,
         encode_login_result/1,
         encode_channel_list_result/1,
         encode_channel_join_result/1,
         encode_channel_leave_result/1,
         encode_channel_send_result/1,
         encode_channel_push/4,
         encode_channel_push_batch/1,
         encode_private_send_result/1,
         encode_private_push/3,
         encode_move_result/1,
         encode_teleport_result/1,
         encode_nearby_send_result/1,
         encode_nearby_push/5,
         encode_map_join_result/1,
         encode_map_leave_result/1,
         encode_map_chat_send_result/1,
         encode_map_chat_push/4,
         encode_error/2]).

decode_request(<<?PROTO_LOGIN_REQUEST:16, NameLength:16, Data/binary>>) ->
    case Data of
        <<RoleName:NameLength/binary, Password/binary>> ->
            {ok, {login, RoleName, Password}};
        _ ->
            {error, {invalid_packet, ?PROTO_LOGIN_REQUEST}}
    end;
decode_request(<<?PROTO_LOGIN_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_LOGIN_REQUEST}};
decode_request(<<?PROTO_CHANNEL_LIST_REQUEST:16>>) ->
    {ok, list_channels};
decode_request(<<?PROTO_CHANNEL_LIST_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_CHANNEL_LIST_REQUEST}};
decode_request(<<?PROTO_CHANNEL_JOIN_REQUEST:16, ChannelId:32>>) ->
    {ok, {join_channel, ChannelId}};
decode_request(<<?PROTO_CHANNEL_JOIN_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_CHANNEL_JOIN_REQUEST}};
decode_request(<<?PROTO_CHANNEL_LEAVE_REQUEST:16, ChannelId:32>>) ->
    {ok, {leave_channel, ChannelId}};
decode_request(<<?PROTO_CHANNEL_LEAVE_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_CHANNEL_LEAVE_REQUEST}};
decode_request(<<?PROTO_CHANNEL_SEND_REQUEST:16,
                 ChannelId:32, Content/binary>>) ->
    {ok, {send_channel, ChannelId, Content}};
decode_request(<<?PROTO_CHANNEL_SEND_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_CHANNEL_SEND_REQUEST}};
decode_request(<<?PROTO_PRIVATE_SEND_REQUEST:16,
                 TargetNameLength:16, Data/binary>>) ->
    case Data of
        <<TargetRoleName:TargetNameLength/binary, Content/binary>> ->
            {ok, {send_private, TargetRoleName, Content}};
        _ ->
            {error, {invalid_packet, ?PROTO_PRIVATE_SEND_REQUEST}}
    end;
decode_request(<<?PROTO_PRIVATE_SEND_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_PRIVATE_SEND_REQUEST}};
decode_request(<<?PROTO_MAP_MOVE_REQUEST:16, Direction:8>>) ->
    {ok, {move, decode_direction(Direction)}};
decode_request(<<?PROTO_MAP_MOVE_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_MAP_MOVE_REQUEST}};
decode_request(<<?PROTO_MAP_TELEPORT_REQUEST:16, X:8, Y:8>>) ->
    {ok, {teleport, {X, Y}}};
decode_request(<<?PROTO_MAP_TELEPORT_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_MAP_TELEPORT_REQUEST}};
decode_request(<<?PROTO_NEARBY_SEND_REQUEST:16, Content/binary>>) ->
    {ok, {send_nearby, Content}};
decode_request(<<?PROTO_MAP_JOIN_REQUEST:16, MapId:16>>) ->
    {ok, {join_map, MapId}};
decode_request(<<?PROTO_MAP_JOIN_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_MAP_JOIN_REQUEST}};
decode_request(<<?PROTO_MAP_LEAVE_REQUEST:16>>) ->
    {ok, leave_map};
decode_request(<<?PROTO_MAP_LEAVE_REQUEST:16, _Data/binary>>) ->
    {error, {invalid_packet, ?PROTO_MAP_LEAVE_REQUEST}};
decode_request(<<?PROTO_MAP_CHAT_SEND_REQUEST:16, Content/binary>>) ->
    {ok, {send_map, Content}};
decode_request(<<ProtoId:16, Data/binary>>) ->
    case is_request_proto(ProtoId) of
        true -> {ok, {request, ProtoId, Data}};
        false -> {error, {unknown_proto, ProtoId}}
    end;
decode_request(_Packet) ->
    {error, {invalid_packet, 0}}.

encode_login_result({ok, RoleId, {X, Y}, ChannelIds}) ->
    ChannelCount = length(ChannelIds),
    ChannelData = << <<ChannelId:32>> || ChannelId <- ChannelIds >>,
    <<?PROTO_LOGIN_RESULT:16, ?RESULT_SUCCESS:8, RoleId:32,
      X:8, Y:8, ChannelCount:16, ChannelData/binary>>;
encode_login_result({error, invalid_login}) ->
    <<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_INVALID_LOGIN:8>>;
encode_login_result({error, already_online}) ->
    <<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_ALREADY_ONLINE:8>>;
encode_login_result({error, service_unavailable}) ->
    <<?PROTO_LOGIN_RESULT:16, ?LOGIN_RESULT_SERVICE_UNAVAILABLE:8>>.

encode_channel_list_result(ChannelList) ->
    ChannelCount = length(ChannelList),
    ChannelData = iolist_to_binary([
        begin
            NameLength = byte_size(ChannelName),
            <<ChannelId:32, ChannelType:8, Joined:8,
              NameLength:16, ChannelName/binary>>
        end
     || {ChannelId, ChannelType, Joined, ChannelName} <- ChannelList]),
    <<?PROTO_CHANNEL_LIST_RESULT:16, ChannelCount:16, ChannelData/binary>>.

encode_channel_join_result({ok, ChannelId}) ->
    <<?PROTO_CHANNEL_JOIN_RESULT:16, ?RESULT_SUCCESS:8, ChannelId:32>>;
encode_channel_join_result({error, invalid_channel, ChannelId}) ->
    <<?PROTO_CHANNEL_JOIN_RESULT:16,
      ?CHANNEL_JOIN_RESULT_INVALID_CHANNEL:8, ChannelId:32>>;
encode_channel_join_result({error, already_joined, ChannelId}) ->
    <<?PROTO_CHANNEL_JOIN_RESULT:16,
      ?CHANNEL_JOIN_RESULT_ALREADY_JOINED:8, ChannelId:32>>;
encode_channel_join_result({error, channel_unavailable, ChannelId}) ->
    <<?PROTO_CHANNEL_JOIN_RESULT:16,
      ?CHANNEL_JOIN_RESULT_UNAVAILABLE:8, ChannelId:32>>.

encode_channel_leave_result({ok, ChannelId}) ->
    <<?PROTO_CHANNEL_LEAVE_RESULT:16, ?RESULT_SUCCESS:8, ChannelId:32>>;
encode_channel_leave_result({error, invalid_channel, ChannelId}) ->
    <<?PROTO_CHANNEL_LEAVE_RESULT:16,
      ?CHANNEL_LEAVE_RESULT_INVALID_CHANNEL:8, ChannelId:32>>;
encode_channel_leave_result({error, not_joined, ChannelId}) ->
    <<?PROTO_CHANNEL_LEAVE_RESULT:16,
      ?CHANNEL_LEAVE_RESULT_NOT_JOINED:8, ChannelId:32>>;
encode_channel_leave_result({error, cannot_leave_main, ChannelId}) ->
    <<?PROTO_CHANNEL_LEAVE_RESULT:16,
      ?CHANNEL_LEAVE_RESULT_CANNOT_LEAVE_MAIN:8, ChannelId:32>>;
encode_channel_leave_result({error, channel_unavailable, ChannelId}) ->
    <<?PROTO_CHANNEL_LEAVE_RESULT:16,
      ?CHANNEL_LEAVE_RESULT_UNAVAILABLE:8, ChannelId:32>>.

encode_channel_send_result({ok, ChannelId}) ->
    <<?PROTO_CHANNEL_SEND_RESULT:16, ?RESULT_SUCCESS:8, ChannelId:32>>;
encode_channel_send_result({error, invalid_channel, ChannelId}) ->
    <<?PROTO_CHANNEL_SEND_RESULT:16,
      ?CHANNEL_SEND_RESULT_INVALID_CHANNEL:8, ChannelId:32>>;
encode_channel_send_result({error, not_joined, ChannelId}) ->
    <<?PROTO_CHANNEL_SEND_RESULT:16,
      ?CHANNEL_SEND_RESULT_NOT_JOINED:8, ChannelId:32>>;
encode_channel_send_result({error, broadcast_failed, ChannelId}) ->
    <<?PROTO_CHANNEL_SEND_RESULT:16,
      ?CHANNEL_SEND_RESULT_BROADCAST_FAILED:8, ChannelId:32>>;
encode_channel_send_result({error, channel_unavailable, ChannelId}) ->
    <<?PROTO_CHANNEL_SEND_RESULT:16,
      ?CHANNEL_SEND_RESULT_UNAVAILABLE:8, ChannelId:32>>.

encode_channel_push(ChannelId, SenderRoleId, SenderRoleName, Content) ->
    SenderNameLength = byte_size(SenderRoleName),
    <<?PROTO_CHANNEL_PUSH:16, ChannelId:32, SenderRoleId:32,
      SenderNameLength:16, SenderRoleName/binary, Content/binary>>.

encode_channel_push_batch(Packets) ->
    PacketData = [<<(byte_size(Packet)):32, Packet/binary>>
                  || Packet <- Packets],
    iolist_to_binary([
        <<?PROTO_CHANNEL_PUSH_BATCH:16, (length(Packets)):16>>,
        PacketData
    ]).

encode_private_send_result({ok, TargetRoleName}) ->
    encode_private_send_result(?RESULT_SUCCESS, TargetRoleName);
encode_private_send_result({error, target_offline, TargetRoleName}) ->
    encode_private_send_result(?PRIVATE_SEND_RESULT_TARGET_OFFLINE,
                               TargetRoleName).

encode_private_push(SenderRoleId, SenderRoleName, Content) ->
    SenderNameLength = byte_size(SenderRoleName),
    <<?PROTO_PRIVATE_PUSH:16, SenderRoleId:32,
      SenderNameLength:16, SenderRoleName/binary, Content/binary>>.

encode_move_result({ok, {X, Y}}) ->
    <<?PROTO_MAP_MOVE_RESULT:16, ?RESULT_SUCCESS:8, X:8, Y:8>>;
encode_move_result({error, invalid_direction, {X, Y}}) ->
    <<?PROTO_MAP_MOVE_RESULT:16,
      ?MAP_MOVE_RESULT_INVALID_DIRECTION:8, X:8, Y:8>>;
encode_move_result({error, out_of_bounds, {X, Y}}) ->
    <<?PROTO_MAP_MOVE_RESULT:16,
      ?MAP_MOVE_RESULT_OUT_OF_BOUNDS:8, X:8, Y:8>>;
encode_move_result({error, not_in_map}) ->
    <<?PROTO_MAP_MOVE_RESULT:16,
      ?MAP_MOVE_RESULT_NOT_IN_MAP:8, 0:8, 0:8>>.

encode_teleport_result({ok, {X, Y}}) ->
    <<?PROTO_MAP_TELEPORT_RESULT:16, ?RESULT_SUCCESS:8, X:8, Y:8>>;
encode_teleport_result({error, invalid_position, {X, Y}}) ->
    <<?PROTO_MAP_TELEPORT_RESULT:16,
      ?MAP_TELEPORT_RESULT_INVALID_POSITION:8, X:8, Y:8>>;
encode_teleport_result({error, not_in_map}) ->
    <<?PROTO_MAP_TELEPORT_RESULT:16,
      ?MAP_TELEPORT_RESULT_NOT_IN_MAP:8, 0:8, 0:8>>.

encode_nearby_send_result({ok, TargetCount}) ->
    <<?PROTO_NEARBY_SEND_RESULT:16, ?RESULT_SUCCESS:8, TargetCount:32>>;
encode_nearby_send_result({error, not_in_map}) ->
    <<?PROTO_NEARBY_SEND_RESULT:16, ?NEARBY_SEND_RESULT_NOT_IN_MAP:8>>.

encode_nearby_push(SenderRoleId, SenderRoleName, X, Y, Content) ->
    SenderNameLength = byte_size(SenderRoleName),
    <<?PROTO_NEARBY_PUSH:16, SenderRoleId:32, X:8, Y:8,
      SenderNameLength:16, SenderRoleName/binary, Content/binary>>.

encode_map_join_result({ok, MapId, {X, Y}}) ->
    <<?PROTO_MAP_JOIN_RESULT:16, ?RESULT_SUCCESS:8, MapId:16, X:8, Y:8>>;
encode_map_join_result({error, invalid_map, MapId}) ->
    <<?PROTO_MAP_JOIN_RESULT:16, ?MAP_JOIN_RESULT_INVALID_MAP:8, MapId:16>>;
encode_map_join_result({error, already_in_map, MapId}) ->
    <<?PROTO_MAP_JOIN_RESULT:16,
      ?MAP_JOIN_RESULT_ALREADY_IN_MAP:8, MapId:16>>;
encode_map_join_result({error, map_unavailable, MapId}) ->
    <<?PROTO_MAP_JOIN_RESULT:16,
      ?MAP_JOIN_RESULT_UNAVAILABLE:8, MapId:16>>.

encode_map_leave_result({ok, MapId}) ->
    <<?PROTO_MAP_LEAVE_RESULT:16, ?RESULT_SUCCESS:8, MapId:16>>;
encode_map_leave_result({error, not_in_map}) ->
    <<?PROTO_MAP_LEAVE_RESULT:16, ?MAP_LEAVE_RESULT_NOT_IN_MAP:8, 0:16>>;
encode_map_leave_result({error, map_unavailable, MapId}) ->
    <<?PROTO_MAP_LEAVE_RESULT:16,
      ?MAP_LEAVE_RESULT_UNAVAILABLE:8, MapId:16>>.

encode_map_chat_send_result({ok, MapId}) ->
    <<?PROTO_MAP_CHAT_SEND_RESULT:16, ?RESULT_SUCCESS:8, MapId:16>>;
encode_map_chat_send_result({error, not_in_map}) ->
    <<?PROTO_MAP_CHAT_SEND_RESULT:16,
      ?MAP_CHAT_SEND_RESULT_NOT_IN_MAP:8, 0:16>>;
encode_map_chat_send_result({error, map_unavailable, MapId}) ->
    <<?PROTO_MAP_CHAT_SEND_RESULT:16,
      ?MAP_CHAT_SEND_RESULT_UNAVAILABLE:8, MapId:16>>.

encode_map_chat_push(MapId, SenderRoleId, SenderRoleName, Content) ->
    SenderNameLength = byte_size(SenderRoleName),
    <<?PROTO_MAP_CHAT_PUSH:16, MapId:16, SenderRoleId:32,
      SenderNameLength:16, SenderRoleName/binary, Content/binary>>.

encode_private_send_result(ResultCode, TargetRoleName) ->
    TargetNameLength = byte_size(TargetRoleName),
    <<?PROTO_PRIVATE_SEND_RESULT:16, ResultCode:8,
      TargetNameLength:16, TargetRoleName/binary>>.

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
is_request_proto(?PROTO_MAP_MOVE_REQUEST) -> true;
is_request_proto(?PROTO_MAP_TELEPORT_REQUEST) -> true;
is_request_proto(?PROTO_NEARBY_SEND_REQUEST) -> true;
is_request_proto(?PROTO_MAP_JOIN_REQUEST) -> true;
is_request_proto(?PROTO_MAP_LEAVE_REQUEST) -> true;
is_request_proto(?PROTO_MAP_CHAT_SEND_REQUEST) -> true;
is_request_proto(_ProtoId) -> false.

decode_direction(?MAP_DIRECTION_UP) -> up;
decode_direction(?MAP_DIRECTION_DOWN) -> down;
decode_direction(?MAP_DIRECTION_LEFT) -> left;
decode_direction(?MAP_DIRECTION_RIGHT) -> right;
decode_direction(_Direction) -> invalid.
