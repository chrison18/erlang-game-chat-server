-module(chat_listener).
-behaviour(gen_server).

%% 只负责 accept 和 Socket 所有权交接，协议处理由 role_server 完成。

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2, terminate/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Port = application:get_env(chat, port, 5555),
    Options = [binary, {packet, 4}, {active, false}, {reuseaddr, true}],
    case gen_tcp:listen(Port, Options) of
        {ok, ListenSocket} ->
            {ok, #{listen_socket => ListenSocket},
             {continue, accept}};
        {error, Reason} ->
            {stop, {listen_failed, Port, Reason}}
    end.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_continue(accept, #{listen_socket := ListenSocket} = State) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            handoff_socket(Socket),
            {noreply, State, {continue, accept}};
        {error, closed} ->
            {stop, normal, State};
        {error, Reason} ->
            {stop, {accept_failed, Reason}, State}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{listen_socket := ListenSocket}) ->
    gen_tcp:close(ListenSocket).

handoff_socket(Socket) ->
    case role_sup:start_role() of
        {ok, RolePid} ->
            %% 必须先转移 controlling process，再通知 Role 开启 active 模式。
            case gen_tcp:controlling_process(Socket, RolePid) of
                ok ->
                    RolePid ! {socket_ready, Socket},
                    ok;
                {error, Reason} ->
                    gen_tcp:close(Socket),
                    gen_server:stop(
                        RolePid, {socket_handoff_failed, Reason}, 5000)
            end;
        {error, _Reason} ->
            gen_tcp:close(Socket)
    end.
