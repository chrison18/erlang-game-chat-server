-module(chat_metrics).

%% 汇总进程邮箱、ETS 计数和地图操作耗时，供采样。

-include("chat_record.hrl").

-export([snapshot/0, online_clients/0, print_online_clients/0,
         record_world_broadcast_delivery/2, map_diagnostic_snapshot/0]).

map_diagnostic_snapshot() ->
    #{operations => maps:from_list([
          {Key, #{count => Count,
                  total_us => TotalUs,
                  avg_us => average(TotalUs, Count)}}
       || {Key, Count, TotalUs} <- table_rows(map_operation_metrics)]),
      batches => maps:from_list([
          {MapId, #{flushes => Flushes,
                    messages => Messages,
                    max_batch => MaxBatch,
                    role_casts => RoleCasts,
                    total_us => TotalUs,
                    max_us => MaxUs,
                    avg_us => average(TotalUs, Flushes)}}
       || {MapId, Flushes, Messages, MaxBatch, RoleCasts, TotalUs, MaxUs} <-
              table_rows(map_batch_metrics)])}.

snapshot() ->
    %% snapshot 只读取当前状态；缺失的监督者或 ETS 会按空数据处理。
    {RoleCount, RoleQueueMax} =
        queue_stats(child_pids(role_sup, role_server)),
    {ChannelCount, ChannelQueueMax} =
        queue_stats(child_pids(chat_sup, channel_server) ++
                    child_pids(channel_sup, channel_server)),
    MapServerCount = length(child_pids(map_server_sup, map_server)),
    MapServerQueues = maps:from_list([
        {MapId, process_queue_length(whereis(map_server:server_name(MapId)))}
     || MapId <- map_server:map_ids()]),
    {WorkerCount, WorkerQueueMax} =
        queue_stats(child_pids(channel_sup, world_broadcast_worker)),
    ChannelBatchStats = channel_batch_stats(),
    {WorldFlushes, WorldRolePackets, WorldPayloadBytes} =
        world_broadcast_delivery_stats(),
    #{node => node(),
      schedulers_online => erlang:system_info(schedulers_online),
      online_count => table_size(online_roles),
      world_member_count => lists:sum([
          table_size(Table) || Table <- channel_server:world_member_tables()]),
      role_count => RoleCount,
      role_queue_max => RoleQueueMax,
      channel_count => ChannelCount,
      channel_queue_max => ChannelQueueMax,
      map_server_count => MapServerCount,
      map_server_queues => MapServerQueues,
      world_worker_count => WorkerCount,
      world_worker_queue_max => WorkerQueueMax,
      channel_timer_flushes => maps:get(timer, ChannelBatchStats),
      channel_full_flushes => maps:get(full, ChannelBatchStats),
      channel_member_sends => maps:get(member_change, ChannelBatchStats),
      channel_batch_messages => maps:get(messages, ChannelBatchStats),
      channel_batch_max => maps:get(max, ChannelBatchStats),
      world_batch_flushes => WorldFlushes,
      world_role_packets => WorldRolePackets,
      world_payload_bytes => WorldPayloadBytes,
      map_operations => add_average(map_server:operation_stats()),
      beam_process_count => erlang:system_info(process_count),
      beam_port_count => erlang:system_info(port_count),
      beam_memory_mb => erlang:memory(total) / (1024 * 1024)}.

record_world_broadcast_delivery(RolePackets, PayloadBytes) ->
    try ets:update_counter(
            broadcast_delivery_metrics, world,
            [{2, 1}, {3, RolePackets}, {4, PayloadBytes}]) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

add_average(Operations) ->
    maps:map(
        fun(_Operation, #{count := Count, total_us := TotalUs} = Stats) ->
            Stats#{avg_us => average(TotalUs, Count)}
        end,
        Operations).

average(_Total, 0) -> 0;
average(Total, Count) -> Total / Count.

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
      map_pid => proplists:get_value(map_pid, Dictionary),
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
    lists:foldl(fun add_queue_length/2, {0, 0}, Pids).

add_queue_length(Pid, {Count, Max}) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Length} ->
            {Count + 1, erlang:max(Max, Length)};
        undefined ->
            {Count, Max}
    end.

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

world_broadcast_delivery_stats() ->
    try ets:lookup(broadcast_delivery_metrics, world) of
        [{world, Flushes, RolePackets, PayloadBytes}] ->
            {Flushes, RolePackets, PayloadBytes};
        [] ->
            {0, 0, 0}
    catch
        error:badarg -> {0, 0, 0}
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

process_queue_length(Pid) when is_pid(Pid) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Length} -> Length;
        undefined -> undefined
    end;
process_queue_length(_Pid) ->
    undefined.
