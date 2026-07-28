-module(channel_manager).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/0, register_channel/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_channel(ChannelId, Type, Name, ChannelPid) ->
    gen_server:call(?MODULE, {register_channel, ChannelId, Type, Name, ChannelPid}).

init([]) ->
    channel_info = ets:new(channel_info, [
        named_table,
        set,
        protected,
        {keypos, #channel_info.channel_id}
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
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.
