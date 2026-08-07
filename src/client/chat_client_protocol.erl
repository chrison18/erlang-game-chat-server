-module(chat_client_protocol).

-include("chat_protocol.hrl").

-export([encode_login/2,
         encode_channel_list/0,
         encode_channel_join/1,
         encode_channel_leave/1,
         encode_channel_send/2,
         encode_private_send/2,
         decode_packet/1]).

encode_login(RoleName, Password) ->
    NameLength = byte_size(RoleName),
    <<?PROTO_LOGIN_REQUEST:16, NameLength:16,
      RoleName/binary, Password/binary>>.

encode_channel_list() ->
    <<?PROTO_CHANNEL_LIST_REQUEST:16>>.

encode_channel_join(ChannelId) ->
    <<?PROTO_CHANNEL_JOIN_REQUEST:16, ChannelId:32>>.

encode_channel_leave(ChannelId) ->
    <<?PROTO_CHANNEL_LEAVE_REQUEST:16, ChannelId:32>>.

encode_channel_send(ChannelId, Content) ->
    <<?PROTO_CHANNEL_SEND_REQUEST:16, ChannelId:32, Content/binary>>.

encode_private_send(TargetRoleName, Content) ->
    TargetNameLength = byte_size(TargetRoleName),
    <<?PROTO_PRIVATE_SEND_REQUEST:16, TargetNameLength:16,
      TargetRoleName/binary, Content/binary>>.

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
decode_packet(<<?PROTO_CHANNEL_LIST_RESULT:16,
                ChannelCount:16, ChannelData/binary>>) ->
    case decode_channels(ChannelCount, ChannelData, []) of
        {ok, Channels} ->
            {ok, {channel_list_result, {ok, Channels}}};
        error ->
            {error, invalid_packet}
    end;
decode_packet(<<?PROTO_CHANNEL_JOIN_RESULT:16,
                ResultCode:8, ChannelId:32>>) ->
    {ok, {channel_join_result,
          decode_channel_join_result(ResultCode, ChannelId)}};
decode_packet(<<?PROTO_CHANNEL_JOIN_RESULT:16, _Data/binary>>) ->
    {error, invalid_packet};
decode_packet(<<?PROTO_CHANNEL_LEAVE_RESULT:16,
                ResultCode:8, ChannelId:32>>) ->
    {ok, {channel_leave_result,
          decode_channel_leave_result(ResultCode, ChannelId)}};
decode_packet(<<?PROTO_CHANNEL_LEAVE_RESULT:16, _Data/binary>>) ->
    {error, invalid_packet};
decode_packet(<<?PROTO_CHANNEL_SEND_RESULT:16,
                ResultCode:8, ChannelId:32>>) ->
    {ok, {channel_send_result,
          decode_channel_send_result(ResultCode, ChannelId)}};
decode_packet(<<?PROTO_CHANNEL_SEND_RESULT:16, _Data/binary>>) ->
    {error, invalid_packet};
decode_packet(<<?PROTO_CHANNEL_PUSH_BATCH:16,
                MessageCount:16, MessageData/binary>>) ->
    decode_channel_push_batch(MessageCount, MessageData, []);
decode_packet(<<?PROTO_CHANNEL_PUSH:16, ChannelId:32, SenderRoleId:32,
                SenderNameLength:16, Data/binary>>) ->
    case Data of
        <<SenderRoleName:SenderNameLength/binary, Content/binary>> ->
            {ok, {channel_push, #{channel_id => ChannelId,
                                  sender_role_id => SenderRoleId,
                                  sender_role_name => SenderRoleName,
                                  content => Content}}};
        _ ->
            {error, invalid_packet}
    end;
decode_packet(<<?PROTO_CHANNEL_PUSH:16, _Data/binary>>) ->
    {error, invalid_packet};
decode_packet(<<?PROTO_PRIVATE_SEND_RESULT:16, ResultCode:8,
                TargetNameLength:16, Data/binary>>) ->
    case Data of
        <<TargetRoleName:TargetNameLength/binary>> ->
            {ok, {private_send_result,
                  decode_private_send_result(ResultCode, TargetRoleName)}};
        _ ->
            {error, invalid_packet}
    end;
decode_packet(<<?PROTO_PRIVATE_SEND_RESULT:16, _Data/binary>>) ->
    {error, invalid_packet};
decode_packet(<<?PROTO_PRIVATE_PUSH:16, SenderRoleId:32,
                SenderNameLength:16, Data/binary>>) ->
    case Data of
        <<SenderRoleName:SenderNameLength/binary, Content/binary>> ->
            {ok, {private_push, #{sender_role_id => SenderRoleId,
                                  sender_role_name => SenderRoleName,
                                  content => Content}}};
        _ ->
            {error, invalid_packet}
    end;
decode_packet(<<?PROTO_PRIVATE_PUSH:16, _Data/binary>>) ->
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

decode_channel_push_batch(0, <<>>, Messages) ->
    {ok, {channel_push_batch, lists:reverse(Messages)}};
decode_channel_push_batch(Count,
                          <<PacketLength:32, Data/binary>>,
                          Messages)
  when Count > 0 ->
    case Data of
        <<Packet:PacketLength/binary, RemainingData/binary>> ->
            case decode_packet(Packet) of
                {ok, {channel_push, Message}} ->
                    decode_channel_push_batch(
                        Count - 1, RemainingData, [Message | Messages]);
                _Error ->
                    {error, invalid_packet}
            end;
        _ ->
            {error, invalid_packet}
    end;
decode_channel_push_batch(_Count, _Data, _Messages) ->
    {error, invalid_packet}.

decode_channels(0, <<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_channels(Count,
                <<ChannelId:32, ChannelTypeValue:8, JoinedValue:8,
                  NameLength:16, Data/binary>>,
                Acc)
  when Count > 0 ->
    case Data of
        <<ChannelName:NameLength/binary, RemainingData/binary>>
          when JoinedValue =:= 0; JoinedValue =:= 1 ->
            Channel = #{channel_id => ChannelId,
                        channel_type => decode_channel_type(ChannelTypeValue),
                        joined => JoinedValue =:= 1,
                        channel_name => ChannelName},
            decode_channels(Count - 1, RemainingData, [Channel | Acc]);
        _ ->
            error
    end;
decode_channels(_Count, _Data, _Acc) ->
    error.

decode_channel_type(?CHANNEL_TYPE_MAIN) -> main;
decode_channel_type(?CHANNEL_TYPE_PUBLIC) -> public;
decode_channel_type(ChannelType) -> {unknown, ChannelType}.

decode_channel_join_result(?RESULT_SUCCESS, ChannelId) ->
    {ok, ChannelId};
decode_channel_join_result(?CHANNEL_JOIN_RESULT_INVALID_CHANNEL, ChannelId) ->
    {error, invalid_channel, ChannelId};
decode_channel_join_result(?CHANNEL_JOIN_RESULT_ALREADY_JOINED, ChannelId) ->
    {error, already_joined, ChannelId};
decode_channel_join_result(ResultCode, ChannelId) ->
    {error, {unknown_result, ResultCode}, ChannelId}.

decode_channel_leave_result(?RESULT_SUCCESS, ChannelId) ->
    {ok, ChannelId};
decode_channel_leave_result(?CHANNEL_LEAVE_RESULT_INVALID_CHANNEL, ChannelId) ->
    {error, invalid_channel, ChannelId};
decode_channel_leave_result(?CHANNEL_LEAVE_RESULT_NOT_JOINED, ChannelId) ->
    {error, not_joined, ChannelId};
decode_channel_leave_result(?CHANNEL_LEAVE_RESULT_CANNOT_LEAVE_MAIN, ChannelId) ->
    {error, cannot_leave_main, ChannelId};
decode_channel_leave_result(ResultCode, ChannelId) ->
    {error, {unknown_result, ResultCode}, ChannelId}.

decode_channel_send_result(?RESULT_SUCCESS, ChannelId) ->
    {ok, ChannelId};
decode_channel_send_result(?CHANNEL_SEND_RESULT_INVALID_CHANNEL, ChannelId) ->
    {error, invalid_channel, ChannelId};
decode_channel_send_result(?CHANNEL_SEND_RESULT_NOT_JOINED, ChannelId) ->
    {error, not_joined, ChannelId};
decode_channel_send_result(?CHANNEL_SEND_RESULT_BROADCAST_FAILED, ChannelId) ->
    {error, broadcast_failed, ChannelId};
decode_channel_send_result(ResultCode, ChannelId) ->
    {error, {unknown_result, ResultCode}, ChannelId}.

decode_private_send_result(?RESULT_SUCCESS, TargetRoleName) ->
    {ok, TargetRoleName};
decode_private_send_result(?PRIVATE_SEND_RESULT_TARGET_OFFLINE, TargetRoleName) ->
    {error, target_offline, TargetRoleName};
decode_private_send_result(ResultCode, TargetRoleName) ->
    {error, {unknown_result, ResultCode}, TargetRoleName}.
