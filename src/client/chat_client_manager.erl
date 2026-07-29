-module(chat_client_manager).

-export([start_client/2, stop_client/1, list_clients/0]).

start_client(Host, Port) ->
    chat_client_sup:start_client(Host, Port).

stop_client(ClientPid) ->
    gen_server:stop(ClientPid).

list_clients() ->
    [Pid
     || {_Id, Pid, worker, [chat_client]} <-
            supervisor:which_children(chat_client_sup),
        is_pid(Pid)].
