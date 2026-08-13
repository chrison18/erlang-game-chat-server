-ifndef(CHAT_RECORD_HRL).
-define(CHAT_RECORD_HRL, true).

-record(role_account, {
    role_name,
    role_id,
    password
}).

-record(online_role, {
    role_name,
    role_pid
}).

-record(channel_state, {
    channel_id,
    channel_type,
    members = #{},
    member_monitors = #{},
    packets = [],
    batch_size = 0,
    batch_generation = 0,
    flush_ref = undefined
}).

-record(channel_member, {
    role_pid,
    socket,
    writer,
    monitor_ref,
    batch_generation = 0,
    batch_start = 0
}).

-record(world_channel_member, {
    role_id,
    role_pid,
    socket,
    writer
}).

-record(map_channel_member, {
    role_id,
    owner_pid,
    role_pid,
    writer,
    batch_generation = 0,
    batch_start = 0
}).

-endif.
