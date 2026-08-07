# chat

一个使用 Erlang/OTP 和 `gen_tcp` 实现的单节点 TCP 聊天项目。

## 编译和启动

```bash
./scripts/compile.sh
./scripts/start_server.sh
```

在另一个终端启动客户端 Shell：

```bash
./scripts/start_client.sh
```

项目使用 `erlc` 和 Shell 脚本，不使用 `rebar3`。服务端包含账号与在线角色管理、10 个固定频道、8 个 `main` 广播 Worker，以及每个连接独立的 `role_server`。

## 客户端操作

客户端 Shell 通过无状态的 `chat_load_test` 模块批量启动客户端。每个普通客户端创建
后使用 `client_N` 作为账号名自动登录；登录成功后每隔 3000ms 随机执行一次动作：
40% 发 `main`、40% 私聊、10% 加入频道、10% 退出频道。无可用目标时退化为发送
`main`。所有客户端使用相同间隔，不按 ClientId 设置首次偏移。

```erlang
ok = chat_load_test:start_observer().
{ok, 2} = chat_load_test:start(1, 2).
ok = chat_load_test:send_channel(1, 1, <<"hello">>).
ok = chat_load_test:send_private(1, 2, <<"hello">>).
```

`chat_load_test` 不是进程，不保存 ClientId 到 PID 的映射。它通过 `chat_client_sup`
启动客户端，并在手工操作时从 supervisor 子进程中查找 ClientId。每个 `chat_client`
自己持有连接和状态，自行登录并推进动作循环。`observer_001` 独立启动且不参与自动
发送；观察者逐条打印频道消息，并周期输出已处理消息数和无效协议数。

`main` 发送通过成员校验并确认 8 个 Worker 均存在后返回成功，接收者 fan-out
异步执行。8 个 Worker 是对等的接收者分片：每个 Worker 按 80ms 或 256 条消息组批，
只遍历自己约八分之一的成员。每个 Role 对一个批次只发送一次 `2010` Packet，客户端
再按顺序处理其中的 `2009` 频道消息。
自动循环会持续产生消息。应从很小的客户端数量开始验证，再逐步增加规模。

服务端 Shell 使用 `chat_metrics:snapshot().` 手动采集当前在线数、关键进程邮箱和 BEAM 资源数据。该接口只在调用时读取状态，不启动常驻统计进程。

当前架构、接口和流程图见 [Chat V1.2 设计文档](<docs/Chat V1.2 设计文档.md>)，性能过程和 10000 客户端结果见 [Chat V1.2 调优报告](<docs/Chat V1.2 调优报告.md>)。根目录的 V1.1 文档仅作历史保留，不再维护。
