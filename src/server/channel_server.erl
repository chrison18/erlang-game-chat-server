-module(channel_server).
-behaviour(gen_server).

-include("chat_protocol.hrl").
-include("chat_record.hrl").

-export([child_spec/1,
         channels/0,
         channel/1,
         start_link/2,
         join/3,
         leave/2,
         send_channel/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

child_spec(ChannelId) ->
    {ok, ChannelType, _ChannelName} = channel(ChannelId),
    #{id => {channel_server, ChannelId},
      start => {channel_server, start_link,
                [ChannelId, ChannelType]}}.

channels() ->
    [channel_tuple(ChannelId) || ChannelId <- lists:seq(1, 10)].

channel(1) -> {ok, ?CHANNEL_TYPE_MAIN, <<"main">>};
channel(2) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_1">>};
channel(3) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_2">>};
channel(4) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_3">>};
channel(5) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_4">>};
channel(6) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_5">>};
channel(7) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_6">>};
channel(8) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_7">>};
channel(9) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_8">>};
channel(10) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_9">>};
channel(_ChannelId) -> error.

start_link(ChannelId, Type) ->
    gen_server:start_link(
        {local, server_name(ChannelId)}, ?MODULE, [ChannelId, Type], []).

join(ChannelId, RoleId, RolePid) ->
    case channel(ChannelId) of
        {ok, _ChannelType, _ChannelName} ->
            gen_server:call(
                server_name(ChannelId), {join, RoleId, RolePid});
        error ->
            {error, invalid_channel}
    end.

leave(1, _RoleId) ->
    {error, cannot_leave_main};
leave(ChannelId, RoleId) ->
    case channel(ChannelId) of
        {ok, _ChannelType, _ChannelName} ->
            gen_server:call(server_name(ChannelId), {leave, RoleId});
        error ->
            {error, invalid_channel}
    end.

send_channel(1, RoleId, RoleName, Content) ->
    world_broadcast_worker:send(RoleId, RoleName, Content);
send_channel(ChannelId, RoleId, RoleName, Content) ->
    case channel(ChannelId) of
        {ok, ?CHANNEL_TYPE_PUBLIC, _ChannelName} ->
            gen_server:call(
                server_name(ChannelId),
                {send_channel, RoleId, RoleName, Content});
        error ->
            {error, invalid_channel}
    end.

init([ChannelId, Type]) ->
    ok = create_world_members(Type),
    {ok, #channel_state{
        channel_id = ChannelId,
        channel_type = Type
    }}.

handle_call({join, RoleId, RolePid}, _From,
            #channel_state{channel_id = ChannelId,
                           channel_type = ChannelType,
                           members = Members,
                           member_monitors = MemberMonitors} = State) ->
    case maps:is_key(RoleId, Members) of
        true ->
            {reply, {error, already_joined}, State};
        false ->
            MonitorRef = erlang:monitor(process, RolePid),
            Member = #channel_member{
                role_pid = RolePid,
                monitor_ref = MonitorRef
            },
            ok = add_world_member(ChannelType, RoleId, RolePid),
            {reply, {ok, ChannelId},
             State#channel_state{
                 members = Members#{RoleId => Member},
                 member_monitors = MemberMonitors#{MonitorRef => RoleId}
             }}
    end;
handle_call({leave, RoleId}, _From,
            #channel_state{channel_id = ChannelId,
                           channel_type = ChannelType,
                           members = Members,
                           member_monitors = MemberMonitors} = State) ->
    case maps:take(RoleId, Members) of
        {#channel_member{monitor_ref = MonitorRef}, RemainingMembers} ->
            true = erlang:demonitor(MonitorRef, [flush]),
            ok = remove_world_member(ChannelType, RoleId),
            {reply, {ok, ChannelId},
             State#channel_state{
                 members = RemainingMembers,
                 member_monitors = maps:remove(MonitorRef, MemberMonitors)
             }};
        error ->
            {reply, {error, not_joined}, State}
    end;
handle_call({send_channel, RoleId, RoleName, Content}, _From,
            #channel_state{channel_id = ChannelId, members = Members} = State) ->
    case maps:is_key(RoleId, Members) of
        false ->
            {reply, {error, not_joined}, State};
        true ->
            maps:foreach(
                fun(_MemberRoleId, #channel_member{role_pid = MemberRolePid}) ->
                    gen_server:cast(MemberRolePid, {
                        push_channel,
                        ChannelId,
                        RoleId,
                        RoleName,
                        Content
                    })
                end,
                Members
            ),
            {reply, {ok, ChannelId}, State}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _RolePid, _Reason},
            #channel_state{channel_type = ChannelType,
                           members = Members,
                           member_monitors = MemberMonitors} = State) ->
    case maps:take(MonitorRef, MemberMonitors) of
        {RoleId, RemainingMonitors} ->
            ok = remove_world_member(ChannelType, RoleId),
            {noreply, State#channel_state{
                members = maps:remove(RoleId, Members),
                member_monitors = RemainingMonitors
            }};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

create_world_members(?CHANNEL_TYPE_MAIN) ->
    world_channel_members = ets:new(world_channel_members, [
        named_table,
        set,
        protected,
        {keypos, #world_channel_member.role_id},
        {read_concurrency, true}
    ]),
    ok;
create_world_members(?CHANNEL_TYPE_PUBLIC) ->
    ok.

add_world_member(?CHANNEL_TYPE_MAIN, RoleId, RolePid) ->
    true = ets:insert(world_channel_members, #world_channel_member{
        role_id = RoleId,
        role_pid = RolePid
    }),
    ok;
add_world_member(?CHANNEL_TYPE_PUBLIC, _RoleId, _RolePid) ->
    ok.

remove_world_member(?CHANNEL_TYPE_MAIN, RoleId) ->
    true = ets:delete(world_channel_members, RoleId),
    ok;
remove_world_member(?CHANNEL_TYPE_PUBLIC, _RoleId) ->
    ok.

channel_tuple(ChannelId) ->
    {ok, ChannelType, ChannelName} = channel(ChannelId),
    {ChannelId, ChannelType, ChannelName}.

server_name(1) -> main_channel_server;
server_name(2) -> public_channel_server_1;
server_name(3) -> public_channel_server_2;
server_name(4) -> public_channel_server_3;
server_name(5) -> public_channel_server_4;
server_name(6) -> public_channel_server_5;
server_name(7) -> public_channel_server_6;
server_name(8) -> public_channel_server_7;
server_name(9) -> public_channel_server_8;
server_name(10) -> public_channel_server_9.
