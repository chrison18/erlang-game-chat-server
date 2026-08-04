-module(world_broadcast_worker).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/1, send/3, worker_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(WORLD_CHANNEL_ID, 1).
-define(WORKER_COUNT, 8).

start_link(WorkerIndex) ->
    gen_server:start_link(?MODULE, [WorkerIndex], []).

send(RoleId, RoleName, Content) ->
    WorkerIndex = erlang:phash2(RoleId, ?WORKER_COUNT) + 1,
    case find_worker(WorkerIndex) of
        {ok, WorkerPid} ->
            call_worker(WorkerPid, {broadcast, RoleId, RoleName, Content});
        error ->
            {error, broadcast_failed}
    end.

worker_count() ->
    ?WORKER_COUNT.

init([WorkerIndex]) ->
    ok = channel_manager:register_world_worker(WorkerIndex, self()),
    {ok, #{worker_index => WorkerIndex}}.

handle_call({broadcast, RoleId, RoleName, Content}, _From, State) ->
    case ets:member(world_channel_members, RoleId) of
        false ->
            {reply, {error, not_joined}, State};
        true ->
            broadcast(RoleId, RoleName, Content),
            {reply, {ok, ?WORLD_CHANNEL_ID}, State}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

find_worker(WorkerIndex) ->
    try channel_manager:world_worker(WorkerIndex)
    catch
        error:badarg -> error
    end.

call_worker(WorkerPid, Request) ->
    try gen_server:call(WorkerPid, Request) of
        Reply -> Reply
    catch
        exit:_Reason -> {error, broadcast_failed}
    end.

broadcast(SenderRoleId, SenderRoleName, Content) ->
    Packet = chat_server_protocol:encode_channel_push(
        ?WORLD_CHANNEL_ID, SenderRoleId, SenderRoleName, Content),
    ets:foldl(
        fun(#world_channel_member{role_pid = MemberRolePid}, ok) ->
            gen_server:cast(MemberRolePid, {push_channel_packet, Packet}),
            ok
        end,
        ok,
        world_channel_members
    ).
