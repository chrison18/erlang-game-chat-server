-module(channel_server_tests).

-include_lib("eunit/include/eunit.hrl").
-include("chat_protocol.hrl").
-include("chat_record.hrl").

public_channel_membership_and_batch_test_() ->
    {setup,
     fun start_public_channel/0,
     fun stop_public_channel/1,
     fun(ChannelPid) ->
         fun() ->
             RoleId = 101,
             RoleName = <<"alice">>,
             Content = <<"hello public">>,
             ?assertEqual({ok, 2},
                          channel_server:join(2, RoleId, self())),
             ?assertEqual({error, already_joined},
                          channel_server:join(2, RoleId, self())),
             ?assertEqual({error, not_joined},
                          channel_server:send_channel(
                              2, 202, <<"bob">>, <<"ignored">>)),
             ?assertEqual({error, not_joined},
                          channel_server:leave(2, 202)),
             ?assertEqual({ok, 2},
                          channel_server:send_channel(
                              2, RoleId, RoleName, Content)),
             flush_channel_batch(ChannelPid),
             ?assertEqual(
                 {ok, {channel_push_batch,
                       [#{channel_id => 2,
                          sender_role_id => RoleId,
                          sender_role_name => RoleName,
                          content => Content}]}},
                 receive_push()),
             ?assertEqual({ok, 2}, channel_server:leave(2, RoleId)),
             ?assertEqual({error, not_joined},
                          channel_server:send_channel(
                              2, RoleId, RoleName, <<"after leave">>)),
             ?assertEqual(timeout, receive_push())
         end
     end}.

start_public_channel() ->
    create_test_table(online_roles),
    create_test_table(channel_batch_metrics),
    {ok, ChannelPid} = channel_server:start_link(2, ?CHANNEL_TYPE_PUBLIC),
    ChannelPid.

stop_public_channel(ChannelPid) ->
    gen_server:stop(ChannelPid),
    ets:delete(online_roles),
    ets:delete(channel_batch_metrics).

create_test_table(Table) ->
    undefined = ets:whereis(Table),
    ets:new(Table, [named_table, public, set]),
    ok.

flush_channel_batch(ChannelPid) ->
    #channel_state{flush_ref = FlushRef} = sys:get_state(ChannelPid),
    ChannelPid ! {timeout, FlushRef, flush_batch},
    ok.

receive_push() ->
    receive
        {'$gen_cast', {push_batch, Packet}} ->
            chat_client_protocol:decode_packet(Packet)
    after 500 ->
        timeout
    end.
