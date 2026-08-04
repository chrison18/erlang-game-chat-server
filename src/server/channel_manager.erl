-module(channel_manager).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/0,
         register_channel/4,
         register_world_worker/2,
         add_world_member/2,
         remove_world_member/1,
         reset_world_members/0,
         world_worker/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_channel(ChannelId, Type, Name, ChannelPid) ->
    gen_server:call(?MODULE, {register_channel, ChannelId, Type, Name, ChannelPid}).

register_world_worker(WorkerIndex, WorkerPid) ->
    gen_server:call(?MODULE, {register_world_worker, WorkerIndex, WorkerPid}).

add_world_member(RoleId, RolePid) ->
    gen_server:call(?MODULE, {add_world_member, RoleId, RolePid}).

remove_world_member(RoleId) ->
    gen_server:call(?MODULE, {remove_world_member, RoleId}).

reset_world_members() ->
    gen_server:call(?MODULE, reset_world_members).

world_worker(WorkerIndex) ->
    case ets:lookup(world_broadcast_workers, WorkerIndex) of
        [#world_broadcast_worker{worker_pid = WorkerPid}] ->
            {ok, WorkerPid};
        [] ->
            error
    end.

init([]) ->
    channel_info = ets:new(channel_info, [
        named_table,
        set,
        protected,
        {keypos, #channel_info.channel_id}
    ]),
    world_channel_members = ets:new(world_channel_members, [
        named_table,
        set,
        protected,
        {keypos, #world_channel_member.role_id},
        {read_concurrency, true}
    ]),
    world_broadcast_workers = ets:new(world_broadcast_workers, [
        named_table,
        set,
        protected,
        {keypos, #world_broadcast_worker.worker_index},
        {read_concurrency, true}
    ]),
    {ok, #{}}.

handle_call({register_channel, ChannelId, Type, Name, ChannelPid}, _From, State) ->
    true = ets:insert(channel_info, #channel_info{
        channel_id = ChannelId,
        channel_type = Type,
        channel_name = Name,
        channel_pid = ChannelPid
    }),
    {reply, ok, State};
handle_call({register_world_worker, WorkerIndex, WorkerPid}, _From, State) ->
    true = ets:insert(world_broadcast_workers, #world_broadcast_worker{
        worker_index = WorkerIndex,
        worker_pid = WorkerPid
    }),
    {reply, ok, State};
handle_call({add_world_member, RoleId, RolePid}, _From, State) ->
    true = ets:insert(world_channel_members, #world_channel_member{
        role_id = RoleId,
        role_pid = RolePid
    }),
    {reply, ok, State};
handle_call({remove_world_member, RoleId}, _From, State) ->
    true = ets:delete(world_channel_members, RoleId),
    {reply, ok, State};
handle_call(reset_world_members, _From, State) ->
    true = ets:delete_all_objects(world_channel_members),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.
