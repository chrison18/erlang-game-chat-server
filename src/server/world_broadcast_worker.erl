-module(world_broadcast_worker).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/1, send/3, worker_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(WORLD_CHANNEL_ID, 1).
-define(WORKER_COUNT, 8).

start_link(WorkerIndex) ->
    gen_server:start_link(
        {local, worker_name(WorkerIndex)}, ?MODULE, [], []).

send(RoleId, RoleName, Content) ->
    WorkerIndex = erlang:phash2(RoleId, ?WORKER_COUNT) + 1,
    call_worker(
        worker_name(WorkerIndex),
        {broadcast, RoleId, RoleName, Content}).

worker_count() ->
    ?WORKER_COUNT.

init([]) ->
    {ok, undefined}.

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

call_worker(Worker, Request) ->
    try gen_server:call(Worker, Request) of
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

worker_name(1) -> world_broadcast_worker_1;
worker_name(2) -> world_broadcast_worker_2;
worker_name(3) -> world_broadcast_worker_3;
worker_name(4) -> world_broadcast_worker_4;
worker_name(5) -> world_broadcast_worker_5;
worker_name(6) -> world_broadcast_worker_6;
worker_name(7) -> world_broadcast_worker_7;
worker_name(8) -> world_broadcast_worker_8.
