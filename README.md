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

两个启动脚本会在启动 BEAM 前启用不限大小的 core dump，并把 `nofile` 软、硬上限都设置为 `65535`。若当前用户无权设置该上限，脚本会停止启动。

项目使用 `erlc` 和 Shell 脚本，不使用 `rebar3`。服务端包含账号与在线角色管理、10 个固定聊天频道、8 个 `main` 广播 Worker、10 个 `map_server`，以及每个连接独立的 `role_server`。固定地图为 `1..10`，大小均为 `100 x 100`。

## 客户端操作

客户端 Shell 通过无状态的 `chat_load_test` 模块批量启动客户端。每个普通客户端创建
后使用 `client_N` 作为账号名自动登录；登录成功后每隔 1000ms 随机执行一次动作：
频道聊天、私聊、移动一格、随机传送、周围聊天、地图聊天、退出当前地图并加入另一张
地图，七种动作等概率。
无可用私聊目标时退化为发送 `main`。所有客户端使用相同间隔，不按 ClientId 设置首次偏移。

```erlang
ok = chat_load_test:start_observer().
{ok, 2} = chat_load_test:start(1, 2).
{ok, 2} = chat_load_test:start_map(1001, 1002).
ok = chat_load_test:send_channel(1, 1, <<"hello">>).
ok = chat_load_test:send_private(1, 2, <<"hello">>).
ok = chat_load_test:set_feedback(1, true).
chat_load_test:position(1).
chat_load_test:location(1).
ok = chat_load_test:move(1, down).
ok = chat_load_test:teleport(1, 20, 30).
ok = chat_load_test:send_nearby(1, <<"hello nearby">>).
ok = chat_load_test:send_map(1, <<"hello map">>).
ok = chat_load_test:leave_map(1).
ok = chat_load_test:join_map(1, 2).
```

普通客户端默认不打印结果，避免压测时淹没 Shell。手工测试时可对指定客户端调用
`set_feedback/2`；服务端回复到达后会打印操作成功或失败，`position/1` 返回最近一次
服务端已确认的位置。再次调用 `set_feedback(ClientId, false)` 可恢复静默。

`chat_load_test` 不是进程，不保存 ClientId 到 PID 的映射。它通过 `chat_client_sup`
启动客户端，并在手工操作时从 supervisor 子进程中查找 ClientId。每个 `chat_client`
自己持有连接和状态，自行登录并推进动作循环。`observer_001` 独立启动且不参与自动
发送；观察者逐条打印频道、地图和周围聊天推送，并周期输出已处理消息数和无效协议数。

`start_map/2` 启动独立的地图压测行为，不改变 `start/2` 的原混合负载。地图客户端
登录后先随机传送到 `0..99` 范围内的坐标，收到成功结果后才开始每 1000ms 执行一次
动作：50% 随机移动、50% 发送附近聊天。

登录默认随机进入地图 `1`。一个角色同时只能处于一张地图；地图聊天只广播给当前地图
玩家，周围聊天只查询同地图当前位置九宫格，不发送进入或离开视野提醒。

公共频道短暂不可用时返回 `channel_unavailable`，失败不会修改 Role 或客户端缓存。
地图状态由各自 `map_server` 进程独占维护，当前假设地图进程不会崩溃，不处理地图
进程重启后的状态恢复。登录初始化期间遇到服务不可用会返回 `service_unavailable`，
关闭当前连接并清理半登录状态。

`main` 发送通过成员校验并确认 8 个 Worker 均存在后返回成功，接收者 fan-out
异步执行。8 个 Worker 是对等的接收者分片：每个 Worker 按 120ms 或 256 条消息组批，
只遍历自己约八分之一的成员。每个 Role 对一个批次只发送一次 `2010` Packet，客户端
再按顺序处理其中的 `2009` 频道消息。

普通公共频道在自己的 `channel_server` 内按 120ms 或 256 条组批，沿用 `2010` 包装
`2009`。地图聊天和周围聊天由对应 `map_server` 直接校验成员并投递单条推送；私聊
仍按单条推送。
自动循环会持续产生消息。应从很小的客户端数量开始验证，再逐步增加规模。

地图请求由 `role_server` 直接转发给登录或切图时保存的 `map_server_N` PID；各地图进程
独立处理加入、退出、移动、传送、nearby 和地图聊天。服务端 Shell 使用
`chat_metrics:snapshot().` 手动采集当前在线数、Role、频道、地图和世界 Worker 邮箱，
以及地图操作耗时。`chat_metrics:print_online_clients().` 会打印在线角色、地图、地图 PID
和坐标。接口只在调用时读取状态，不启动常驻统计进程。

当前 V1.4 地图架构决策见 [当前架构重构决策](<docs/当前架构重构决策.md>)；冻结的 V1.2/V1.3 设计文档和旧负载报告仅作为历史记录保留。
