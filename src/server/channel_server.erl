-module(channel_server).
-behaviour(gen_server).

-include("chat_protocol.hrl").
-include("chat_record.hrl").

-export([start_link/3, join/3, leave/2, send_channel/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(ChannelId, Type, Name) ->
    gen_server:start_link(?MODULE, [ChannelId, Type, Name], []).

join(ChannelPid, RoleId, RolePid) ->
    gen_server:call(ChannelPid, {join, RoleId, RolePid}).

leave(ChannelPid, RoleId) ->
    gen_server:call(ChannelPid, {leave, RoleId}).

send_channel(#channel_info{channel_type = ?CHANNEL_TYPE_MAIN},
             RoleId, RoleName, Content) ->
    world_broadcast_worker:send(RoleId, RoleName, Content);
send_channel(#channel_info{channel_pid = ChannelPid},
             RoleId, RoleName, Content) ->
    gen_server:call(ChannelPid, {send_channel, RoleId, RoleName, Content}).

init([ChannelId, Type, Name]) ->
    ok = reset_world_members(Type),
    ok = channel_manager:register_channel(ChannelId, Type, Name, self()),
    {ok, #channel_state{
        channel_id = ChannelId,
        channel_type = Type,
        channel_name = Name
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
                role_id = RoleId,
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

reset_world_members(?CHANNEL_TYPE_MAIN) ->
    channel_manager:reset_world_members();
reset_world_members(?CHANNEL_TYPE_PUBLIC) ->
    ok.

add_world_member(?CHANNEL_TYPE_MAIN, RoleId, RolePid) ->
    channel_manager:add_world_member(RoleId, RolePid);
add_world_member(?CHANNEL_TYPE_PUBLIC, _RoleId, _RolePid) ->
    ok.

remove_world_member(?CHANNEL_TYPE_MAIN, RoleId) ->
    channel_manager:remove_world_member(RoleId);
remove_world_member(?CHANNEL_TYPE_PUBLIC, _RoleId) ->
    ok.
