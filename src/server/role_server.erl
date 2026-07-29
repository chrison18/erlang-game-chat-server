-module(role_server).
-behaviour(gen_server).

-include("chat_protocol.hrl").
-include("chat_record.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #{socket => undefined}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({socket_ready, Socket}, #{socket := undefined} = State) ->
    case inet:setopts(Socket, [{active, once}]) of
        ok ->
            {noreply, State#{socket := Socket}};
        {error, Reason} ->
            {stop, {socket_activation_failed, Reason}, State#{socket := Socket}}
    end;
handle_info({tcp, Socket, Packet}, #{socket := Socket} = State) ->
    case handle_packet(Packet, Socket) of
        ok ->
            case inet:setopts(Socket, [{active, once}]) of
                ok ->
                    {noreply, State};
                {error, Reason} ->
                    {stop, {socket_activation_failed, Reason}, State}
            end;
        {error, Reason} ->
            {stop, Reason, State}
    end;
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, {tcp_error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := undefined}) ->
    ok;
terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket).

handle_packet(Packet, Socket) ->
    case chat_server_protocol:decode_request(Packet) of
        {ok, {login, RoleName, Password}} ->
            handle_login(Socket, RoleName, Password);
        {ok, {request, ProtoId, _Data}} ->
            case get(role_id) of
                undefined -> send_packet(Socket,
                    chat_server_protocol:encode_error(ProtoId, not_logged_in));
                _RoleId -> send_packet(Socket,
                    chat_server_protocol:encode_error(ProtoId, unknown_proto))
            end;
        {error, {invalid_packet, ProtoId}} ->
            send_packet(Socket,
                chat_server_protocol:encode_error(ProtoId, invalid_packet));
        {error, {unknown_proto, ProtoId}} ->
            send_packet(Socket,
                chat_server_protocol:encode_error(ProtoId, unknown_proto))
    end.

handle_login(Socket, RoleName, Password) ->
    case get(role_id) of
        undefined ->
            case role_online_server:login(self(), RoleName, Password) of
                {ok, RoleId} ->
                    ChannelIds = join_initial_channels(RoleId),
                    put(role_id, RoleId),
                    put(role_name, RoleName),
                    put(channel_ids,
                        maps:from_list([{ChannelId, true} || ChannelId <- ChannelIds])),
                    send_packet(Socket,
                        chat_server_protocol:encode_login_result(
                            {ok, RoleId, ChannelIds}));
                {error, Reason} ->
                    send_packet(Socket,
                        chat_server_protocol:encode_login_result({error, Reason}))
            end;
        _RoleId ->
            send_packet(Socket,
                chat_server_protocol:encode_login_result(
                    {error, already_online}))
    end.

join_initial_channels(RoleId) ->
    PublicCount = rand:uniform(3),
    RandomizedPublicIds = [
        ChannelId
     || {_RandomKey, ChannelId} <-
            lists:sort([{rand:uniform(), Id} || Id <- lists:seq(2, 10)])
    ],
    ChannelIds = [1 | lists:sublist(RandomizedPublicIds, PublicCount)],
    lists:foreach(
        fun(ChannelId) ->
            {ok, ChannelId} = join_channel(ChannelId, RoleId)
        end,
        ChannelIds
    ),
    ChannelIds.

join_channel(ChannelId, RoleId) ->
    case ets:lookup(channel_info, ChannelId) of
        [#channel_info{channel_pid = ChannelPid}] ->
            channel_server:join(ChannelPid, RoleId, self());
        [] ->
            {error, invalid_channel}
    end.

send_packet(Socket, Packet) ->
    case gen_tcp:send(Socket, Packet) of
        ok -> ok;
        {error, Reason} -> {error, {tcp_send_failed, Reason}}
    end.
