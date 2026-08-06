# Chat V1.1 接口文档

## 1. 文档说明

本文档用于快速了解和验收 Chat V1.1 的外部接口。项目内部监督树、进程关系、ETS 结构和完整二进制报文格式见 [Chat V1.1 设计文档](<Chat V1.1 设计文档.md>)。

V1.1 提供以下功能：

- TCP 客户端连接与断开
- 账号首次登录自动创建、密码校验和重复在线检查
- 查询、加入和退出固定频道
- 频道聊天和私聊
- 批量启动自动行动客户端和独立观察者
- 角色下线后的在线记录与频道成员清理

## 2. 编译与启动

编译：

```bash
./scripts/compile.sh
```

启动服务端：

```bash
./scripts/start_server.sh
```

启动客户端 Shell：

```bash
./scripts/start_client.sh
```

默认监听地址为本机，端口为 `5555`。服务端和客户端 Shell 应在不同终端中运行。

## 3. 客户端操作接口

| 接口 | 返回值 | 说明 |
|---|---|---|
| `chat_load_test:start(StartId, EndId)` | `{ok, Count} \| {error, Reason}` | 串行创建首尾 ID 范围内的普通客户端；每个客户端自行登录并进入固定间隔动作循环 |
| `chat_load_test:start_observer()` | `ok \| {error, Reason}` | 独立创建账号为 `observer_001` 的观察者并自动登录 |
| `chat_load_test:send_channel(ClientId, ChannelId, Content)` | `ok \| {error, Reason}` | 指定客户端发送频道消息 |
| `chat_load_test:send_private(SenderId, TargetId, Content)` | `ok \| {error, Reason}` | 指定客户端向目标客户端私聊 |

示例：

```erlang
ok = chat_load_test:start_observer().
{ok, 100} = chat_load_test:start(1, 100).
ok = chat_load_test:send_channel(1, 1, <<"hello main">>).
ok = chat_load_test:send_private(1, 2, <<"hello client 2">>).
```

`start(1, 100)` 包含起止 ID，共创建 100 个客户端。客户端使用 `client_1` 到
`client_100` 作为账号名。每个客户端连接后自行登录，登录成功后使用 `send_after`
安排第一条 `main` 消息；每次发送完成后再安排下一次动作，固定间隔为 3000ms。

`start/2` 返回 `{ok, Count}` 只表示这些客户端进程已经创建，不表示全部登录响应都已
返回。所有普通客户端使用相同的 3000ms 间隔，不根据 ClientId 计算首次 delay 或
偏移量；实际发送时刻仍受连接、登录响应和 BEAM 调度影响。`observer_001` 不自动
发送消息，应独立启动。

## 4. 内部执行方式

`chat_load_test` 是普通函数模块，不是 GenServer，也不保存 ClientId 与 PID 的映射。
它通过 `chat_client_sup` 启动客户端；手工操作时读取 supervisor 当前子进程并直接向
对应 `chat_client` cast。登录、发包、收包、状态和连续动作都由客户端自身管理。

| 对外接口 | 执行方式或发给 `chat_client` 的消息 |
|---|---|
| `start/2` | supervisor 启动参数包含 ClientId 和登录身份；客户端通过 `handle_continue` 自行登录 |
| `start_observer/0` | supervisor 启动观察者；观察者通过 `handle_continue` 自行登录 |
| `send_channel/3` | `{send_channel, ChannelId, Content}` |
| `send_private/3` | `{send_private, TargetRoleName, Content}` |

## 5. 异步结果约定

`send_channel/3` 和 `send_private/3` 返回 `ok`，只表示操作已经交给对应的
`chat_client`，不表示 TCP 发送或服务端业务处理成功。

服务端响应稍后由 `chat_client` 接收。客户端会更新连接、角色和频道状态，但普通
客户端不在 Shell 中打印业务结果和消息推送。客户端不保存最近一次业务结果，也不
使用 `gen_server:call/3`、`From` 或 `gen_server:reply/2` 向原发送者同步返回结果。

常见结果格式：

| 操作 | 成功结果 | 失败结果示例 |
|---|---|---|
| 登录 | `{ok, RoleId, ChannelIds}` | `{error, invalid_login}`、`{error, already_online}` |
| 查询频道 | `{ok, Channels}` | `{error, Reason}` |
| 加入频道 | `{ok, ChannelId}` | `{error, already_joined, ChannelId}` |
| 退出频道 | `{ok, ChannelId}` | `{error, not_joined, ChannelId}`、`{error, cannot_leave_main, 1}` |
| 频道发送 | `{ok, ChannelId}` | `{error, not_joined, ChannelId}`、`{error, broadcast_failed, ChannelId}` |
| 私聊发送 | `{ok, TargetRoleName}` | `{error, target_offline, TargetRoleName}` |

观察者会打印自己的登录结果和收到的每条频道推送：

```text
[login] {ok,1,[1,3,2]}
[channel 1] client_1(2): client_1 auto message 1
observer=observer_001 received=1 invalid=0
```

汇总信息每秒输出一次并清零当前周期计数：`received` 表示这一秒内观察者实际处理的
频道推送数，`invalid` 表示无法解码的协议包数。它们不代表服务端全局发送总数。
私聊推送当前只解码，不打印也不计入该汇总。

频道推送格式：

```text
[channel 10] alice(1): hello
```

## 6. 服务端压测快照

服务端 Shell 手动调用：

```erlang
chat_metrics:snapshot().
```

接口返回一个 Map，不启动进程、不定时采集也不写文件。主要字段如下：

| 字段 | 含义 |
|---|---|
| `online_count` | `online_roles` ETS 中已登录角色数 |
| `world_member_count` | 已加入 `main` 的角色数 |
| `role_count` | 当前存活的 `role_server` 数量，即服务端连接进程数 |
| `role_queue_total` / `role_queue_max` | 所有 Role 邮箱消息总数和单个最大值 |
| `channel_queue_total` / `channel_queue_max` | 10 个频道进程的邮箱总数和最大值 |
| `world_worker_queue_total` / `world_worker_queue_max` | 8 个世界广播 Worker 的邮箱总数和最大值 |
| `beam_process_count` / `beam_port_count` | 当前 BEAM 进程数和端口数 |
| `beam_memory_mb` | BEAM 当前动态分配内存，单位 MiB |
| `node` / `schedulers_online` | 当前节点名和在线调度器数量 |

快照是调用时的瞬时观测。采样期间进程仍可能上线或退出，因此相关计数不保证来自
完全相同的时间点。

## 7. TCP 协议索引

Socket 使用 `binary`、`{packet, 4}` 和 `{active, true}`。`PacketLength` 由 `gen_tcp` 自动处理，业务层报文从 16 位 `ProtoId` 开始。

| 请求 | 结果或推送 | 功能 |
|---|---|---|
| `1001` | `1002` | 登录 |
| `2001` | `2002` | 查询频道 |
| `2003` | `2004` | 加入频道 |
| `2005` | `2006` | 退出频道 |
| `2007` | `2008`、`2009` | 发送结果、频道消息推送 |
| `3001` | `3002`、`3003` | 私聊结果、私聊消息推送 |
| - | `9001` | 未登录、非法报文或未知协议 |

完整字段和结果码定义见设计文档第 7 章。

## 8. 最短验收示例

在客户端 Shell 中创建两个客户端并操作：

```erlang
ok = chat_load_test:start_observer().
{ok, 2} = chat_load_test:start(1, 2).
ok = chat_load_test:send_channel(1, 1, <<"hello main">>).
ok = chat_load_test:send_private(1, 2, <<"hello client 2">>).
```

验收时确认：

1. 观察者和 ClientId 1、2 对应的客户端均保持在线。
2. 登录后必定加入 `main`，并随机加入 1 到 3 个公共频道。
3. 普通客户端登录成功后无需其他命令，观察者会持续收到它们发出的 `main` 消息。
4. 观察者汇总中的 `invalid` 保持为 `0`。

## 9. V1.1 边界

- 不提供独立注册接口，首次登录自动创建账号。
- 账号、频道和在线数据不持久化，服务端重启后重新初始化。
- 不保存聊天记录，不支持离线私聊。
- 客户端断开后不自动重连。
- 不包含心跳、空闲超时、自动重连和发送循环停止接口。
- 普通客户端固定每 3000ms 发送一次，不提供运行时频率调整接口。
- 观察者逐条打印频道消息，终端 I/O 本身可能在高负载下形成瓶颈；汇总仅反映观察者
  已处理的数据，不是服务端完整性证明。
- 当前没有业务层流量控制或邮箱积压保护。
- TCP 协议不包含 `RequestId`，不提供逐请求同步返回或回调关联。
