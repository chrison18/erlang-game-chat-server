# chat

基于 Erlang/OTP 的单节点高并发 TCP 聊天服务器（游戏后端架构），采用 Actor 模型每连接一进程，支持频道聊天、世界广播、私聊、多地图 AOI 等完整功能。

## 核心亮点

- **万级并发稳定承载**：单节点稳定支撑 10000 活跃客户端混合负载，服务端内存稳定 217 MiB，单进程邮箱最大 1
- **8 分片对等广播**：世界频道按角色 ID 哈希分到 8 张 ETS 表，每个 Worker 独立组批只遍历 1/8 成员，结合协议批量包（120ms / 256 条），将进程邮箱消息与 TCP 发送系统调用减少约两个数量级
- **多地图 AOI 系统**：10 张固定地图每图独立进程并行处理，2×3 逻辑分片 + 九宫格查询，移动/传送只计算 enter/leave 差集避免全量重算
- **容错与可观测**：rest_for_one 监督树保证崩溃恢复一致性，monitor + 反向索引实现 O(1) 断线清理，内置 metrics 快照接口实时采集在线数、进程邮箱、操作耗时

## 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Erlang/OTP |
| 网络 | gen_tcp（二进制协议） |
| 存储 | ETS |
| 进程模型 | gen_server / supervisor / Actor |
| 算法 | AOI 九宫格差集 |

## 架构概览

```
客户端连接 → role_server（每连接独立进程，持有连接与状态）
                ├── channel_server   # 10 个固定频道，组批广播
                ├── main Worker ×8   # 世界频道 8 分片对等广播
                └── map_server ×10   # 每图独立进程，AOI 九宫格 + 地图聊天
```

- 公共频道与世界广播按 120ms 或 256 条组批，单批次只发送一次包装 Packet，客户端按序解包
- 地图请求由 role_server 转发给缓存的 map_server PID，加入/退出同步确认，移动/传送/聊天异步 cast
- 协议调用携带单调时钟截止时间，防止迟到请求副作用

## 快速上手

```bash
# 编译
./scripts/compile.sh

# 启动服务端（自动设置 nofile 上限 65535 + core dump）
./scripts/start_server.sh

# 另起终端，启动压测客户端 Shell
./scripts/start_client.sh
```

压测客户端通过 `chat_load_test` 模块批量启动，支持混合负载、地图压测、AOI 纯移动三种模式，详见使用参考。

## 项目结构

```
src/
├── server/          # 服务端核心（role/channel/map/main_worker）
├── client/          # 压测客户端
├── protocol/        # 二进制协议编解码
└── common/          # 公共工具
scripts/             # 编译与启动脚本
docs/                # 设计文档与调优报告
test/                # 测试
```

## 更多文档

- [使用与压测参考](docs/使用与压测参考.md) — 客户端操作、压测模式、metrics 接口等详细说明
- [当前架构重构决策](docs/当前架构重构决策.md) — V1.4 地图架构决策
- [Chat V1.3 设计文档](docs/Chat%20V1.3%20设计文档.md)
- [Chat V1.2 调优报告](docs/Chat%20V1.2%20调优报告.md)
