# AGENT.md

这个文件给后续维护本项目的 AI Agent 或开发者看。

## 项目简介

`kfcm` 是一个 Kafka 集群管理 CLI，使用 Go 编写。

主要入口文件：

```text
main.go
```

依赖管理：

```text
go.mod
go.sum
```

当前已生成的 Linux 二进制：

```text
kfcm-linux-amd64
```

## 技术栈

- Go
- Cobra：命令行框架
- kafka-go：Kafka 协议客户端
- tabwriter：表格对齐输出

## 命令结构

```text
kfcm
├── cluster
│   ├── add
│   │   └── topic <broker> --name <topic> --partitions <n> --replication-factor <n>
│   ├── list
│   │   ├── <broker>
│   │   ├── topic <broker> [--name <topic>]
│   │   └── consumergroups <broker> [--name <group>] [--with-coordinator]
│   └── delete
│       ├── topic <broker> --name <topic> --yes
│       └── consumergroup <broker> --name <group> --yes
└── debug
    └── describegroup <broker> --name <group>
```

## 开发约定

1. 尽量保持单文件简单实现，当前主要逻辑都在 `main.go`。
2. 新增命令优先接入 Cobra command tree。
3. 表格输出使用 `newTableWriter()`，不要直接用裸 `\t` 输出表格。
4. 删除、变更类操作必须加确认参数，比如 `--yes`。
5. 不要打印密码、token、SASL secret 等敏感信息。
6. 修改后必须运行：

```bash
go test ./...
```

## 编译

Linux amd64：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o kfcm-linux-amd64 .
```

Windows 本地临时检查：

```bash
go build -o kfcm-windows-check.exe .
```

检查完建议删除临时 exe。

## 关键实现说明

### Consumer group 详情

消费组详情没有直接使用 `kafka-go.Client.DescribeGroups()` 的高层解析。

原因：部分客户端，例如 Kafka 自带 console consumer，返回的 member metadata 可能让 kafka-go 严格解析失败，出现类似：

```text
Got non-zero number of bytes remaining: 13
```

所以当前实现使用底层 `describegroups.Request`，再对 assignment 做宽松解析。

### LAG 计算

LAG 计算逻辑：

```text
lag = latest offset - committed offset
```

使用 API：

- `OffsetFetch` 获取消费组 committed offset
- `ListOffsets` 获取 partition latest offset

当前展示的是每个 member 负责 partition 的 LAG 总和。

### Debug 命令

`kfcm debug describegroup` 用于排查 consumer group metadata 差异。

它会输出 raw hex，也会尝试解析 metadata 和 assignment 字段。

排查重点：

```text
MEMBER_METADATA_REMAINING_LENGTH
MEMBER_ASSIGNMENT_REMAINING_LENGTH
```

如果 remaining 不为 0，说明该段 bytes 里有当前解析逻辑没消费掉的内容。

## 后续可能扩展

- 支持 SASL/PLAIN
- 支持 SCRAM
- 支持 TLS
- 支持配置文件保存默认 broker
- 支持查看 topic partition 明细
- 支持查看整个消费组总 LAG
- 支持 JSON 输出，方便脚本调用
