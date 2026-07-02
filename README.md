# kfcm

`kfcm` 是一个用 Go 写的 Kafka 集群管理命令行工具。

它主要用于日常运维场景，比如查看 broker、topic、消费组，创建或删除 topic，查看消费组成员分配和 LAG。

## 功能

- 查看 Kafka broker 列表
- 创建 topic
- 删除 topic
- 查看 topic 列表
- 查看单个 topic 的分区数和副本数
- 查看消费组列表
- 查看消费组详情，包括成员、分区分配、LAG
- 删除消费组
- debug 查看 DescribeGroups 原始 metadata 和 assignment

## 构建

本机当前已生成 Linux amd64 二进制：

```bash
kfcm-linux-amd64
```

手动编译 Linux 版本：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o kfcm-linux-amd64 .
```

编译当前系统版本：

```bash
go build -o kfcm .
```

## 使用

查看帮助：

```bash
kfcm --help
kfcm cluster --help
```

## Broker

列出集群中的 broker：

```bash
kfcm cluster list 10.2.15.150:18080
```

输出示例：

```text
ID        HOST          PORT   RACK
718080    10.2.16.7     18080  r1135
15018080  10.2.15.150   18080  r1134
```

## Topic

创建 topic：

```bash
kfcm cluster add topic 10.2.15.150:18080 --name test-topic --partitions 60 --replication-factor 2
```

列出所有 topic：

```bash
kfcm cluster list topic 10.2.15.150:18080
```

查看单个 topic 的分区数和副本数：

```bash
kfcm cluster list topic 10.2.15.150:18080 --name test-topic
```

输出示例：

```text
TOPIC       PARTITIONS  REPLICATION_FACTOR
test-topic  60          2
```

删除 topic：

```bash
kfcm cluster delete topic 10.2.15.150:18080 --name test-topic --yes
```

说明：删除操作必须带 `--yes`，避免误删。

## Consumer Group

列出消费组：

```bash
kfcm cluster list consumergroups 10.2.33.161:9091
```

默认只显示消费组名：

```text
GROUP
group-logstash-k8s-event
k8s-bizlog
```

如果需要查看 coordinator broker ID：

```bash
kfcm cluster list consumergroups 10.2.33.161:9091 --with-coordinator
```

查看单个消费组详情：

```bash
kfcm cluster list consumergroups 10.2.33.161:9091 --name group-logstash-k8s-event
```

输出示例：

```text
GROUP  group-logstash-k8s-event
STATE  Stable
MEMBER_ID  CLIENT_ID  CLIENT_HOST  ASSIGNMENTS     LAG
xxx        client-1   /10.2.1.1    k8s-event:[0]   12
yyy        client-2   /10.2.1.2    -               -
```

字段说明：

- `STATE`：消费组状态，例如 `Stable`
- `MEMBER_ID`：Kafka 给消费者成员生成的 ID
- `CLIENT_ID`：客户端上报的 client id
- `CLIENT_HOST`：消费者所在主机
- `ASSIGNMENTS`：该消费者负责的 topic partition
- `LAG`：该消费者负责分区的总延迟，计算方式是 `latest offset - committed offset`

删除消费组：

```bash
kfcm cluster delete consumergroup 10.2.33.161:9091 --name test-consumer-group --yes
```

也可以使用别名：

```bash
kfcm cluster delete consumergroups 10.2.33.161:9091 --name test-consumer-group --yes
kfcm cluster delete consumer-group 10.2.33.161:9091 --name test-consumer-group --yes
```

说明：删除消费组要求该消费组没有活跃成员，否则 Kafka 会拒绝删除。

## Debug

查看消费组的原始 DescribeGroups 内容：

```bash
kfcm debug describegroup 10.2.13.35:19080 --name test-consumer-group
```

这个命令主要用于排查不同客户端的 consumer group metadata 差异。

它会输出：

- group 状态
- protocol type
- protocol data
- member id
- client id
- client host
- member metadata 原始 hex
- member assignment 原始 hex
- metadata 按 ConsumerProtocol 解析后的字段
- assignment 按 ConsumerProtocol 解析后的字段

常见关注字段：

```text
MEMBER_METADATA_VERSION
MEMBER_METADATA_TOPICS
MEMBER_METADATA_USER_DATA_LENGTH
MEMBER_METADATA_OWNED_PARTITIONS
MEMBER_METADATA_REMAINING_LENGTH
MEMBER_ASSIGNMENT_PARTITIONS
MEMBER_ASSIGNMENT_REMAINING_LENGTH
```

如果 `MEMBER_METADATA_REMAINING_LENGTH` 不为 0，通常说明该客户端写入的 metadata 和当前解析逻辑不完全一致。

## 注意事项

- 当前工具支持明文 Kafka 连接。
- 如果集群开启了 SASL/TLS，还需要继续扩展连接参数。
- 删除 topic 需要 Kafka broker 开启 `delete.topic.enable=true`。
- 删除消费组时，消费组不能有活跃成员。
- LAG 是按当前 member 负责的 partition 汇总出来的，不是整个消费组总 LAG。

## 依赖

主要依赖：

- `github.com/segmentio/kafka-go`
- `github.com/spf13/cobra`
