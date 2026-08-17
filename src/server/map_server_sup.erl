-module(map_server_sup).
-behaviour(supervisor).

%% 每张固定地图一个 map_server，不同地图的状态修改可以并行。

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    map_operation_metrics = ets:new(
        map_operation_metrics, [named_table, public, set, {write_concurrency, true}]),
    map_batch_metrics = ets:new(
        map_batch_metrics, [named_table, public, set]),
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    {ok, {SupFlags,
          [map_server:child_spec(MapId)
           || MapId <- map_server:map_ids()]}}.
