-module(channel_sup).
-behaviour(supervisor).

-include("chat_protocol.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    MainChannel = channel_child_spec(1, ?CHANNEL_TYPE_MAIN, <<"main">>),
    WorldWorkers = [
        world_worker_child_spec(WorkerIndex)
     || WorkerIndex <- lists:seq(1, world_broadcast_worker:worker_count())],
    PublicChannels = [
        channel_child_spec(
            ChannelId,
            ?CHANNEL_TYPE_PUBLIC,
            list_to_binary("public_" ++ integer_to_list(ChannelId - 1))
        )
     || ChannelId <- lists:seq(2, 10)],
    {ok, {SupFlags, [MainChannel | WorldWorkers] ++ PublicChannels}}.

channel_child_spec(ChannelId, Type, Name) ->
    #{id => {channel_server, ChannelId},
      start => {channel_server, start_link, [ChannelId, Type, Name]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [channel_server]}.

world_worker_child_spec(WorkerIndex) ->
    #{id => {world_broadcast_worker, WorkerIndex},
      start => {world_broadcast_worker, start_link, [WorkerIndex]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [world_broadcast_worker]}.
