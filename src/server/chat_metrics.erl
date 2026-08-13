-module(chat_metrics).

-include("chat_record.hrl").

-export([snapshot/0, online_clients/0, print_online_clients/0,
         record_broadcast_delivery/5,
         record_broadcast_send_failure/1,
         record_broadcast_send_failures/2]).

snapshot() ->
    {RoleCount, RoleQueueTotal, RoleQueueMax} =
        queue_stats(child_pids(role_sup, role_server)),
    {ChannelCount, ChannelQueueTotal, ChannelQueueMax} =
        queue_stats(child_pids(chat_sup, channel_server) ++
                    child_pids(channel_sup, channel_server)),
    {MapWorkerCount, MapWorkerQueueTotal, MapWorkerQueueMax} =
        queue_stats(child_pids(map_worker_sup, map_worker)),
    MapWorkerQueues = maps:from_list([
        {MapId, process_queue_length(whereis(map_worker:server_name(MapId)))}
     || MapId <- map_router:map_ids()]),
    {WorkerCount, WorkerQueueTotal, WorkerQueueMax} =
        queue_stats(child_pids(channel_sup, world_broadcast_worker)),
    {MapBroadcastWorkerCount, MapBroadcastWorkerQueueTotal,
     MapBroadcastWorkerQueueMax} =
        queue_stats(child_pids(channel_sup, map_broadcast_worker)),
    NearbyWorkers = child_pids(channel_sup, nearby_broadcast_worker),
    {NearbyWorkerCount, NearbyQueueTotal, NearbyQueueMax} =
        queue_stats(NearbyWorkers),
    NearbyStats = nearby_stats(),
    ChannelBatchStats = channel_batch_stats(),
    {WorldFlushes, WorldRolePackets, WorldLogicalBytes,
     WorldWireBytes, WorldSendFailures} =
        broadcast_delivery_stats(world),
    {MapFlushes, MapRolePackets, MapLogicalBytes,
     MapWireBytes, MapSendFailures} =
        broadcast_delivery_stats(map),
    {NearbyDeliveryFlushes, NearbyRolePackets, NearbyLogicalBytes,
     NearbyWireBytes, NearbySendFailures} =
        broadcast_delivery_stats(nearby),
    {SocketWriterCount, SocketWriterQueueTotal, SocketWriterQueueMax} =
        socket_writer_queue_stats(),
    {SocketSendPendTotal, SocketSendPendMax} = socket_send_pend_stats(),
    #{node => node(),
      schedulers_online => erlang:system_info(schedulers_online),
      online_count => table_size(online_roles),
      world_member_count => lists:sum([
          table_size(Table) || Table <- channel_server:world_member_tables()]),
      role_count => RoleCount,
      role_queue_total => RoleQueueTotal,
      role_queue_max => RoleQueueMax,
      socket_writer_count => SocketWriterCount,
      socket_writer_queue_total => SocketWriterQueueTotal,
      socket_writer_queue_max => SocketWriterQueueMax,
      socket_send_pend_total => SocketSendPendTotal,
      socket_send_pend_max => SocketSendPendMax,
      channel_count => ChannelCount,
      channel_queue_total => ChannelQueueTotal,
      channel_queue_max => ChannelQueueMax,
      map_router_count => process_count(chat_sup, map_router),
      map_worker_count => MapWorkerCount,
      map_worker_queue_total => MapWorkerQueueTotal,
      map_worker_queue_max => MapWorkerQueueMax,
      map_worker_queues => MapWorkerQueues,
      map_role_counts => maps:from_list([
          {MapId, table_size(map_worker:cell_table(MapId))}
       || MapId <- map_router:map_ids()]),
      world_worker_count => WorkerCount,
      world_worker_queue_total => WorkerQueueTotal,
      world_worker_queue_max => WorkerQueueMax,
      map_broadcast_worker_count => MapBroadcastWorkerCount,
      map_broadcast_worker_queue_total => MapBroadcastWorkerQueueTotal,
      map_broadcast_worker_queue_max => MapBroadcastWorkerQueueMax,
      nearby_worker_count => NearbyWorkerCount,
      nearby_worker_queue_total => NearbyQueueTotal,
      nearby_worker_queue_max => NearbyQueueMax,
      nearby_messages => maps:get(messages, NearbyStats),
      nearby_targets => maps:get(targets, NearbyStats),
      nearby_flushes => maps:get(flushes, NearbyStats),
      nearby_batch_messages => maps:get(batch_messages, NearbyStats),
      nearby_batch_max => maps:get(batch_max, NearbyStats),
      channel_timer_flushes => maps:get(timer, ChannelBatchStats),
      channel_full_flushes => maps:get(full, ChannelBatchStats),
      channel_member_sends => maps:get(member_change, ChannelBatchStats),
      channel_batch_messages => maps:get(messages, ChannelBatchStats),
      channel_batch_max => maps:get(max, ChannelBatchStats),
      world_batch_flushes => WorldFlushes,
      world_role_packets => WorldRolePackets,
      world_logical_bytes => WorldLogicalBytes,
      world_wire_bytes => WorldWireBytes,
      world_payload_bytes => WorldWireBytes,
      world_send_failures => WorldSendFailures,
      map_batch_flushes => MapFlushes,
      map_role_packets => MapRolePackets,
      map_logical_bytes => MapLogicalBytes,
      map_wire_bytes => MapWireBytes,
      map_payload_bytes => MapWireBytes,
      map_send_failures => MapSendFailures,
      nearby_delivery_flushes => NearbyDeliveryFlushes,
      nearby_role_packets => NearbyRolePackets,
      nearby_logical_bytes => NearbyLogicalBytes,
      nearby_wire_bytes => NearbyWireBytes,
      nearby_payload_bytes => NearbyWireBytes,
      nearby_send_failures => NearbySendFailures,
      map_operations => map_router:operation_stats(),
      beam_process_count => erlang:system_info(process_count),
      beam_port_count => erlang:system_info(port_count),
      beam_memory_mb => erlang:memory(total) / (1024 * 1024)}.

record_broadcast_delivery(Type, Flushes, RolePackets,
                          LogicalBytes, WireBytes) ->
    try ets:update_counter(
            broadcast_delivery_metrics, Type,
            [{2, Flushes}, {3, RolePackets},
             {4, LogicalBytes}, {5, WireBytes}]) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

record_broadcast_send_failure(Type) ->
    record_broadcast_send_failures(Type, 1).

record_broadcast_send_failures(_Type, 0) ->
    ok;
record_broadcast_send_failures(Type, Count) ->
    try ets:update_counter(broadcast_delivery_metrics, Type, {6, Count}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

online_clients() ->
    lists:sort([
        online_client(RoleName, RolePid)
     || #online_role{role_name = RoleName, role_pid = RolePid} <-
            ets:tab2list(online_roles)
    ]).

print_online_clients() ->
    Clients = online_clients(),
    lists:foreach(fun(Client) -> io:format("~p~n", [Client]) end, Clients),
    {ok, length(Clients)}.

online_client(RoleName, RolePid) ->
    Dictionary = case process_info(RolePid, dictionary) of
        {dictionary, Values} -> Values;
        undefined -> []
    end,
    QueueLength = case process_info(RolePid, message_queue_len) of
        {message_queue_len, Length} -> Length;
        undefined -> undefined
    end,
    #{role_id => proplists:get_value(role_id, Dictionary),
      role_name => RoleName,
      role_pid => RolePid,
      message_queue_len => QueueLength,
      map_id => proplists:get_value(map_id, Dictionary),
      position => proplists:get_value(position, Dictionary)}.

child_pids(Supervisor, Module) ->
    try supervisor:which_children(Supervisor) of
        Children ->
            [ChildPid
             || {_Id, ChildPid, worker, Modules} <- Children,
                is_pid(ChildPid),
                lists:member(Module, Modules)]
    catch
        exit:_Reason -> []
    end.

queue_stats(Pids) ->
    lists:foldl(fun add_queue_length/2, {0, 0, 0}, Pids).

add_queue_length(Pid, {Count, Total, Max}) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Length} ->
            {Count + 1, Total + Length, erlang:max(Max, Length)};
        undefined ->
            {Count, Total, Max}
    end.

nearby_stats() ->
    lists:foldl(
        fun({_MapId, Messages, Targets, Flushes,
             BatchMessages, BatchMax}, Acc) ->
            Acc#{messages := maps:get(messages, Acc) + Messages,
                 targets := maps:get(targets, Acc) + Targets,
                 flushes := maps:get(flushes, Acc) + Flushes,
                 batch_messages := maps:get(batch_messages, Acc) +
                                   BatchMessages,
                 batch_max := erlang:max(
                     maps:get(batch_max, Acc), BatchMax)}
        end,
        #{messages => 0,
          targets => 0,
          flushes => 0,
          batch_messages => 0,
          batch_max => 0},
        table_rows(nearby_batch_metrics)).

channel_batch_stats() ->
    lists:foldl(
        fun({{_ChannelId, Reason}, Count, Messages, Max}, Acc) ->
            Acc#{Reason := maps:get(Reason, Acc) + Count,
                 messages := maps:get(messages, Acc) + Messages,
                 max := erlang:max(maps:get(max, Acc), Max)}
        end,
        #{timer => 0, full => 0, member_change => 0,
          messages => 0, max => 0},
        table_rows(channel_batch_metrics)).

broadcast_delivery_stats(Type) ->
    case table_rows(broadcast_delivery_metrics) of
        Rows ->
            case lists:keyfind(Type, 1, Rows) of
                {Type, Flushes, RolePackets,
                 LogicalBytes, WireBytes, SendFailures} ->
                    {Flushes, RolePackets, LogicalBytes,
                     WireBytes, SendFailures};
                false ->
                    {0, 0, 0, 0, 0}
            end
    end.

table_size(Table) ->
    case ets:info(Table, size) of
        undefined -> 0;
        Size -> Size
    end.

table_rows(Table) ->
    case ets:info(Table) of
        undefined -> [];
        _ -> ets:tab2list(Table)
    end.

process_count(Supervisor, Module) ->
    length(child_pids(Supervisor, Module)).

process_queue_length(Pid) when is_pid(Pid) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Length} -> Length;
        undefined -> undefined
    end;
process_queue_length(_Pid) ->
    undefined.

socket_send_pend_stats() ->
    lists:foldl(
        fun(#world_channel_member{socket = Socket}, {Total, Max}) ->
            socket_send_pend(Socket, Total, Max)
        end,
        {0, 0},
        lists:append([
            table_rows(Table)
         || Table <- channel_server:world_member_tables()
        ])).

socket_send_pend(Socket, Total, Max) when is_port(Socket) ->
    case catch inet:getstat(Socket, [send_pend]) of
        {ok, [{send_pend, Bytes}]} ->
            {Total + Bytes, erlang:max(Max, Bytes)};
        _ ->
            {Total, Max}
    end;
socket_send_pend(_Socket, Total, Max) ->
    {Total, Max}.

socket_writer_queue_stats() ->
    Writers = [Writer
               || Table <- channel_server:world_member_tables(),
                  #world_channel_member{writer = Writer} <- table_rows(Table),
                  is_pid(Writer)],
    queue_stats(Writers).
