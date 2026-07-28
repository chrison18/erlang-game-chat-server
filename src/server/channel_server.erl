-module(channel_server).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(ChannelId, Type, Name) ->
    gen_server:start_link(?MODULE, [ChannelId, Type, Name], []).

init([ChannelId, Type, Name]) ->
    ok = channel_manager:register_channel(ChannelId, Type, Name, self()),
    {ok, #channel_state{
        channel_id = ChannelId,
        channel_type = Type,
        channel_name = Name
    }}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.
