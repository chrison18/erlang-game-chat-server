-module(socket_writer).

-export([start_link/2, send_sync/2, send_async/3]).

start_link(Socket, Owner) ->
    Pid = spawn_link(fun() -> loop(Socket, Owner) end),
    {ok, Pid}.

send_sync(Pid, Packet) when is_pid(Pid), is_binary(Packet) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! {send_sync, self(), Ref, Packet},
    receive
        {Ref, Result} ->
            erlang:demonitor(Ref, [flush]),
            Result;
        {'DOWN', Ref, process, Pid, Reason} ->
            {error, Reason}
    end;
send_sync(_Pid, _Packet) ->
    {error, unavailable}.

send_async(Pid, Packet, Type)
  when is_pid(Pid), is_binary(Packet) ->
    Pid ! {send, Packet, Type},
    ok;
send_async(_Pid, _Packet, _Type) ->
    {error, unavailable}.

loop(Socket, Owner) ->
    receive
        {send_sync, From, Ref, Packet} ->
            Result = gen_tcp:send(Socket, Packet),
            From ! {Ref, Result},
            case Result of
                ok -> loop(Socket, Owner);
                {error, Reason} -> exit({tcp_send_failed, Reason})
            end;
        {send, Packet, Type} ->
            case gen_tcp:send(Socket, Packet) of
                ok ->
                    loop(Socket, Owner);
                {error, Reason} ->
                    record_send_failure(Type),
                    Owner ! {socket_writer_error, self(), Reason},
                    exit({tcp_send_failed, Reason})
            end;
        stop ->
            ok;
        _Other ->
            loop(Socket, Owner)
    end.

record_send_failure(Type)
  when Type =:= world; Type =:= map; Type =:= nearby ->
    chat_metrics:record_broadcast_send_failure(Type);
record_send_failure(_Type) ->
    ok.
