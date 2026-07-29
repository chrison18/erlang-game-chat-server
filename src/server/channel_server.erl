-module(channel_server).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/3, join/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(ChannelId, Type, Name) ->
    gen_server:start_link(?MODULE, [ChannelId, Type, Name], []).

join(ChannelPid, RoleId, RolePid) ->
    gen_server:call(ChannelPid, {join, RoleId, RolePid}).

init([ChannelId, Type, Name]) ->
    ok = channel_manager:register_channel(ChannelId, Type, Name, self()),
    {ok, #channel_state{
        channel_id = ChannelId,
        channel_type = Type,
        channel_name = Name
    }}.

handle_call({join, RoleId, RolePid}, _From,
            #channel_state{channel_id = ChannelId, members = Members} = State) ->
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
            {reply, {ok, ChannelId},
             State#channel_state{members = Members#{RoleId => Member}}}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _RolePid, _Reason},
            #channel_state{members = Members} = State) ->
    RemainingMembers = maps:filter(
        fun(_RoleId, #channel_member{monitor_ref = MemberMonitorRef}) ->
            MemberMonitorRef =/= MonitorRef
        end,
        Members
    ),
    {noreply, State#channel_state{members = RemainingMembers}};
handle_info(_Info, State) ->
    {noreply, State}.
