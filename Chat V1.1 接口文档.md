# Chat V1.1 接口文档

## 1. 文档说明

本文档用于快速了解和验收 Chat V1.1 的外部接口。项目内部监督树、进程关系、ETS 结构和完整二进制报文格式见 [Chat V1.1 设计文档](<Chat V1.1 设计文档.md>)。

V1.1 提供以下功能：

- TCP 客户端连接与断开
- 账号首次登录自动创建、密码校验和重复在线检查
- 查询、加入和退出固定频道
- 频道聊天和私聊
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
| `client:start_client(StartId, EndId)` | `{ok, Count} \| {error, Reason}` | 创建首尾 ID 范围内的客户端并自动登录 |
| `client:send_channel(ClientId, ChannelId, Content)` | `ok \| {error, Reason}` | 指定客户端发送频道消息 |
| `client:send_private(SenderId, TargetId, Content)` | `ok \| {error, Reason}` | 指定客户端向目标客户端私聊 |

示例：

```erlang
{ok, 100} = client:start_client(1, 100).
ok = client:send_channel(1, 1, <<"hello main">>).
ok = client:send_private(1, 2, <<"hello client 2">>).
```

`start_client(1, 100)` 包含起止 ID，共创建 100 个客户端。客户端使用
`client_1` 到 `client_100` 作为账号名，统一使用 `123456` 作为密码。登录成功后，
每个客户端通过 `send_after` 每隔 1000ms 自动向 `main` 频道发送消息。

## 4. 内部执行方式

`client` 保存 ClientId 与客户端 PID 的对应关系。接口找到对应 PID 后，只向
`chat_client` 发送普通 Erlang 消息，登录、发包、收包和状态更新仍由客户端自身完成。

| 对外接口 | 发给 `chat_client` 的内部消息 |
|---|---|
| `start_client/2` | `{login, RoleName, Password}` |
| `send_channel/3` | `{send_channel, ChannelId, Content}` |
| `send_private/3` | `{send_private, TargetRoleName, Content}` |

## 5. 异步结果约定

`send_channel/3` 和 `send_private/3` 返回 `ok`，只表示操作已经交给对应的
`chat_client`，不表示 TCP 发送或服务端业务处理成功。

服务端响应稍后由 `chat_client` 接收。客户端会更新连接、角色和频道状态，并在 Shell 中打印结果。客户端不保存最近一次业务结果，也不使用 `gen_server:call/3`、`From` 或 `gen_server:reply/2` 向原发送者同步返回结果。

常见结果格式：

| 操作 | 成功结果 | 失败结果示例 |
|---|---|---|
| 登录 | `{ok, RoleId, ChannelIds}` | `{error, invalid_login}`、`{error, already_online}` |
| 查询频道 | `{ok, Channels}` | `{error, Reason}` |
| 加入频道 | `{ok, ChannelId}` | `{error, already_joined, ChannelId}` |
| 退出频道 | `{ok, ChannelId}` | `{error, not_joined, ChannelId}`、`{error, cannot_leave_main, 1}` |
| 频道发送 | `{ok, ChannelId}` | `{error, not_joined, ChannelId}` |
| 私聊发送 | `{ok, TargetRoleName}` | `{error, target_offline, TargetRoleName}` |

打印示例：

```text
[login] {ok,1,[1,3,2]}
[join_channel] {ok,10}
[join_channel] {error,already_joined,10}
[send_private] {error,target_offline,<<"nobody">>}
```

频道和私聊推送格式：

```text
[channel 10] alice(1): hello
[private] alice(1): hello
```

## 6. TCP 协议索引

Socket 使用 `binary`、`{packet, 4}` 和 `{active, once}`。`PacketLength` 由 `gen_tcp` 自动处理，业务层报文从 16 位 `ProtoId` 开始。

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

## 7. 最短验收示例

在客户端 Shell 中创建两个客户端并操作：

```erlang
{ok, 2} = client:start_client(1, 2).
ok = client:send_channel(1, 1, <<"hello main">>).
ok = client:send_private(1, 2, <<"hello client 2">>).
```

验收时确认：

1. ClientId 1 和 2 对应的客户端均能异步登录成功。
2. 登录后必定加入 `main`，并随机加入 1 到 3 个公共频道。
3. 两个客户端都能收到 ClientId 1 发送的 `main` 频道消息。
4. ClientId 2 能收到 ClientId 1 的私聊推送。

## 8. V1.1 边界

- 不提供独立注册接口，首次登录自动创建账号。
- 账号、频道和在线数据不持久化，服务端重启后重新初始化。
- 不保存聊天记录，不支持离线私聊。
- 客户端断开后不自动重连。
- 不包含心跳、空闲超时和自动压力测试。
- TCP 协议不包含 `RequestId`，不提供逐请求同步返回或回调关联。
