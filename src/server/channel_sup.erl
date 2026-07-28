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
    PublicChannels = [
        channel_child_spec(
            ChannelId,
            ?CHANNEL_TYPE_PUBLIC,
            list_to_binary("public_" ++ integer_to_list(ChannelId - 1))
        )
     || ChannelId <- lists:seq(2, 10)],
    {ok, {SupFlags, [MainChannel | PublicChannels]}}.

channel_child_spec(ChannelId, Type, Name) ->
    #{id => {channel_server, ChannelId},
      start => {channel_server, start_link, [ChannelId, Type, Name]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [channel_server]}.
