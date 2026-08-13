-module(channel_sup).
-behaviour(supervisor).

%% 管理广播 Worker、公共频道和每张地图的内部频道。

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% 指标表由监督者持有，单个频道或 Worker 重启不会清空累计数据。
    channel_batch_metrics = ets:new(
        channel_batch_metrics, [named_table, public, set]),
    nearby_batch_metrics = ets:new(
        nearby_batch_metrics, [named_table, public, set]),
    broadcast_delivery_metrics = ets:new(
        broadcast_delivery_metrics,
        [named_table, public, set, {write_concurrency, true}]),
    true = ets:insert(broadcast_delivery_metrics, [
        {world, 0, 0, 0},
        {map, 0, 0, 0},
        {nearby, 0, 0, 0}
    ]),
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    WorldWorkers = [
        world_worker_child_spec(WorkerIndex)
     || WorkerIndex <- lists:seq(1, world_broadcast_worker:worker_count())],
    NearbyWorkers = [
        nearby_worker_child_spec(MapId) || MapId <- map_router:map_ids()],
    PublicChannels = [
        channel_server:child_spec(ChannelId)
     || ChannelId <- lists:seq(2, 10)],
    MapChannels = [
        channel_server:map_child_spec(MapId)
     || MapId <- map_router:map_ids()],
    %% 各类子进程彼此独立，单个广播或频道故障只重启自己。
    {ok, {SupFlags,
          WorldWorkers ++ NearbyWorkers ++ PublicChannels ++ MapChannels}}.

world_worker_child_spec(WorkerIndex) ->
    #{id => {world_broadcast_worker, WorkerIndex},
      start => {world_broadcast_worker, start_link, [WorkerIndex]}}.

nearby_worker_child_spec(MapId) ->
    #{id => {nearby_broadcast_worker, MapId},
      start => {nearby_broadcast_worker, start_link, [MapId]}}.
