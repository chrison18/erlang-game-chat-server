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

客户端 Shell 通过 `client` 模块按 ClientId 操作客户端。每个普通客户端创建后使用
`client_N` 作为账号名、`123456` 作为密码自动登录，并保持静默在线。

```erlang
ok = client:start_observer().
{ok, 2} = client:start_client(1, 2).
{ok, 2} = client:start_send_loop(1, 2).
ok = client:send_channel(1, 1, <<"hello">>).
ok = client:send_private(1, 2, <<"hello">>).
```

`start_send_loop/2` 让指定 ClientId 范围开始每 3000ms 向 `main` 发送消息，适合逐级增加压力；`start_send_loop/0` 启动当前全部普通客户端。首次发送按 ClientId 分散到 3 秒窗口内，重复调用不会创建重复循环。`observer_001` 独立启动且不参与发送；观察者逐条打印频道消息，并周期输出已处理消息数和无效协议数。

服务端 Shell 使用 `chat_metrics:snapshot().` 手动采集当前在线数、关键进程邮箱和 BEAM 资源数据。该接口只在调用时读取状态，不启动常驻统计进程。

完整接口见 [Chat V1.1 接口文档](<Chat V1.1 接口文档.md>)，进程架构、协议和当前性能边界见 [Chat V1.1 设计文档](<Chat V1.1 设计文档.md>)。
