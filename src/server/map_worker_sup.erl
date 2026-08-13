-module(map_worker_sup).
-behaviour(supervisor).

%% 每张固定地图一个写 Worker，不同地图的状态修改可以并行。

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    {ok, {SupFlags,
          [map_worker:child_spec(MapId)
           || MapId <- map_router:map_ids()]}}.
