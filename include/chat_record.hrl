-ifndef(CHAT_RECORD_HRL).
-define(CHAT_RECORD_HRL, true).

-record(role_account, {
    role_name,
    role_id,
    password
}).

-record(online_role, {
    role_name,
    role_id,
    role_pid,
    monitor_ref
}).

-record(channel_info, {
    channel_id,
    channel_type,
    channel_name,
    channel_pid
}).

-record(channel_state, {
    channel_id,
    channel_type,
    channel_name,
    members = #{}
}).

-record(channel_member, {
    role_id,
    role_pid,
    monitor_ref
}).

-endif.
