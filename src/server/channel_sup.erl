-module(channel_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    WorldWorkers = [
        world_worker_child_spec(WorkerIndex)
     || WorkerIndex <- lists:seq(1, world_broadcast_worker:worker_count())],
    PublicChannels = [
        channel_server:child_spec(ChannelId)
     || ChannelId <- lists:seq(2, 10)],
    {ok, {SupFlags, WorldWorkers ++ PublicChannels}}.

world_worker_child_spec(WorkerIndex) ->
    #{id => {world_broadcast_worker, WorkerIndex},
      start => {world_broadcast_worker, start_link, [WorkerIndex]}}.
