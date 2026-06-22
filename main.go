package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
)

const defaultTimeout = 10 * time.Second

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "kfcm",
		Short:         "Kafka cluster management tool",
		SilenceUsage:  true,
		SilenceErrors: true,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	cmd.SetHelpCommand(&cobra.Command{Hidden: true})
	cmd.AddCommand(newClusterCommand())
	return cmd
}

func newClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage Kafka clusters",
		RunE:  showHelp,
	}
	cmd.AddCommand(newClusterAddCommand(), newClusterListCommand())
	return cmd
}

func newClusterAddCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add Kafka cluster resources",
		RunE:  showHelp,
	}
	cmd.AddCommand(newAddTopicCommand())
	return cmd
}

func newAddTopicCommand() *cobra.Command {
	var name string
	var partitions int
	var replicationFactor int

	cmd := &cobra.Command{
		Use:   "topic <broker-ip:port>",
		Short: "Create a Kafka topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("--name is required")
			}
			if partitions <= 0 {
				return fmt.Errorf("--partitions must be greater than 0")
			}
			if replicationFactor <= 0 {
				return fmt.Errorf("--replication-factor must be greater than 0")
			}
			return createTopic(args[0], name, partitions, replicationFactor)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "topic name")
	cmd.Flags().IntVar(&partitions, "partitions", 1, "topic partition count")
	cmd.Flags().IntVar(&replicationFactor, "replication-factor", 1, "topic replication factor")
	return cmd
}

func newClusterListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <broker-ip:port>",
		Short: "List Kafka cluster resources",
		Long: `List Kafka cluster resources.

Usages:
  kfcm cluster list <broker-ip:port>
  kfcm cluster list topic <broker-ip:port>
  kfcm cluster list consumergroups <broker-ip:port>
  kfcm cluster list consumergroups <broker-ip:port> --name <group-name>`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			return listBrokers(args[0])
		},
	}
	cmd.AddCommand(newListTopicCommand(), newListConsumerGroupsCommand())
	return cmd
}

func newListTopicCommand() *cobra.Command {
	var name string

	cmd := &cobra.Command{
		Use:   "topic <broker-ip:port>",
		Short: "List Kafka topics, or describe one topic with --name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(name) != "" {
				return describeTopic(args[0], name)
			}
			return listTopics(args[0])
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "topic name")
	return cmd
}

func newListConsumerGroupsCommand() *cobra.Command {
	var name string
	var withCoordinator bool

	cmd := &cobra.Command{
		Use:   "consumergroups <broker-ip:port>",
		Short: "List Kafka consumer groups, or describe one group with --name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := newClient(args[0])
			if strings.TrimSpace(name) != "" {
				return describeConsumerGroup(client, name)
			}
			return printConsumerGroups(client, withCoordinator)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "consumer group name")
	cmd.Flags().BoolVar(&withCoordinator, "with-coordinator", false, "show coordinator broker ID")
	return cmd
}

func showHelp(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

func createTopic(broker string, name string, partitions int, replicationFactor int) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	client := newClient(broker)

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %q: %w", name, err)
	}
	if topicErr := resp.Errors[name]; topicErr != nil {
		return fmt.Errorf("create topic %q: %w", name, topicErr)
	}

	fmt.Printf("created topic %q partitions=%d replication-factor=%d\n", name, partitions, replicationFactor)
	return nil
}

func listBrokers(broker string) error {
	metadata, err := fetchMetadata(broker)
	if err != nil {
		return err
	}
	brokers := append([]kafka.Broker(nil), metadata.Brokers...)
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })

	w := newTableWriter()
	fmt.Fprintln(w, "ID\tHOST\tPORT\tRACK")
	for _, b := range brokers {
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\n", b.ID, b.Host, b.Port, b.Rack)
	}
	return w.Flush()
}

func listTopics(broker string) error {
	metadata, err := fetchMetadata(broker)
	if err != nil {
		return err
	}
	topics := make([]string, 0, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return fmt.Errorf("topic metadata %q: %w", topic.Name, topic.Error)
		}
		topics = append(topics, topic.Name)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		fmt.Println(topic)
	}
	return nil
}

func describeTopic(broker string, name string) error {
	metadata, err := fetchMetadata(broker, name)
	if err != nil {
		return err
	}
	for _, topic := range metadata.Topics {
		if topic.Name != name {
			continue
		}
		if topic.Error != nil {
			return fmt.Errorf("topic metadata %q: %w", topic.Name, topic.Error)
		}
		w := newTableWriter()
		fmt.Fprintln(w, "TOPIC\tPARTITIONS\tREPLICATION_FACTOR")
		fmt.Fprintf(w, "%s\t%d\t%s\n", topic.Name, len(topic.Partitions), replicationFactorText(topic.Partitions))
		return w.Flush()
	}
	return fmt.Errorf("topic %q not found", name)
}

func replicationFactorText(partitions []kafka.Partition) string {
	if len(partitions) == 0 {
		return "0"
	}
	counts := make(map[int]struct{})
	for _, partition := range partitions {
		counts[len(partition.Replicas)] = struct{}{}
	}
	if len(counts) == 1 {
		for count := range counts {
			return fmt.Sprint(count)
		}
	}
	values := make([]int, 0, len(counts))
	for count := range counts {
		values = append(values, count)
	}
	sort.Ints(values)
	texts := make([]string, 0, len(values))
	for _, value := range values {
		texts = append(texts, fmt.Sprint(value))
	}
	return "mixed(" + strings.Join(texts, ",") + ")"
}

func printConsumerGroups(client *kafka.Client, withCoordinator bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	resp, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return fmt.Errorf("list consumer groups: %w", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("list consumer groups: %w", resp.Error)
	}
	groups := append([]kafka.ListGroupsResponseGroup(nil), resp.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })

	w := newTableWriter()
	if withCoordinator {
		fmt.Fprintln(w, "GROUP\tCOORDINATOR")
		for _, g := range groups {
			fmt.Fprintf(w, "%s\t%d\n", g.GroupID, g.Coordinator)
		}
	} else {
		fmt.Fprintln(w, "GROUP")
		for _, g := range groups {
			fmt.Fprintln(w, g.GroupID)
		}
	}
	return w.Flush()
}

func describeConsumerGroup(client *kafka.Client, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	resp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{name}})
	if err != nil {
		return fmt.Errorf("describe consumer group %q: %w", name, err)
	}
	if len(resp.Groups) == 0 {
		return fmt.Errorf("consumer group %q not found", name)
	}

	for _, g := range resp.Groups {
		if g.Error != nil {
			return fmt.Errorf("describe consumer group %q: %w", name, g.Error)
		}
		lagByPartition, err := fetchGroupLags(client, g.GroupID, g.Members)
		if err != nil {
			return err
		}

		fmt.Printf("GROUP\t%s\n", g.GroupID)
		fmt.Printf("STATE\t%s\n", g.GroupState)

		w := newTableWriter()
		fmt.Fprintln(w, "MEMBER_ID\tCLIENT_ID\tCLIENT_HOST\tASSIGNMENTS\tLAG")
		for _, m := range g.Members {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", m.MemberID, m.ClientID, m.ClientHost, formatAssignments(m.MemberAssignments.Topics), memberLagText(m.MemberAssignments.Topics, lagByPartition))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

type topicPartition struct {
	topic     string
	partition int
}

func fetchGroupLags(client *kafka.Client, groupID string, members []kafka.DescribeGroupsResponseMember) (map[topicPartition]int64, error) {
	partitionsByTopic := make(map[string]map[int]struct{})
	for _, member := range members {
		for _, assignment := range member.MemberAssignments.Topics {
			if _, ok := partitionsByTopic[assignment.Topic]; !ok {
				partitionsByTopic[assignment.Topic] = make(map[int]struct{})
			}
			for _, partition := range assignment.Partitions {
				partitionsByTopic[assignment.Topic][partition] = struct{}{}
			}
		}
	}
	if len(partitionsByTopic) == 0 {
		return map[topicPartition]int64{}, nil
	}

	offsetFetchTopics := make(map[string][]int, len(partitionsByTopic))
	listOffsetTopics := make(map[string][]kafka.OffsetRequest, len(partitionsByTopic))
	for topic, partitions := range partitionsByTopic {
		ids := make([]int, 0, len(partitions))
		for partition := range partitions {
			ids = append(ids, partition)
		}
		sort.Ints(ids)
		offsetFetchTopics[topic] = ids
		requests := make([]kafka.OffsetRequest, 0, len(ids))
		for _, partition := range ids {
			requests = append(requests, kafka.LastOffsetOf(partition))
		}
		listOffsetTopics[topic] = requests
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	committedResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  offsetFetchTopics,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets for consumer group %q: %w", groupID, err)
	}
	if committedResp.Error != nil {
		return nil, fmt.Errorf("fetch committed offsets for consumer group %q: %w", groupID, committedResp.Error)
	}

	latestResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: listOffsetTopics})
	if err != nil {
		return nil, fmt.Errorf("fetch latest offsets for consumer group %q: %w", groupID, err)
	}

	committed := make(map[topicPartition]int64)
	for topic, partitions := range committedResp.Topics {
		for _, partition := range partitions {
			if partition.Error != nil {
				return nil, fmt.Errorf("fetch committed offset %s[%d]: %w", topic, partition.Partition, partition.Error)
			}
			committed[topicPartition{topic: topic, partition: partition.Partition}] = partition.CommittedOffset
		}
	}

	lags := make(map[topicPartition]int64)
	for topic, offsets := range latestResp.Topics {
		for _, offset := range offsets {
			if offset.Error != nil {
				return nil, fmt.Errorf("fetch latest offset %s[%d]: %w", topic, offset.Partition, offset.Error)
			}
			key := topicPartition{topic: topic, partition: offset.Partition}
			committedOffset, ok := committed[key]
			if !ok || committedOffset < 0 {
				lags[key] = -1
				continue
			}
			lag := offset.LastOffset - committedOffset
			if lag < 0 {
				lag = 0
			}
			lags[key] = lag
		}
	}
	return lags, nil
}

func memberLagText(assignments []kafka.GroupMemberTopic, lagByPartition map[topicPartition]int64) string {
	if len(assignments) == 0 {
		return "-"
	}
	var total int64
	assigned := false
	for _, assignment := range assignments {
		for _, partition := range assignment.Partitions {
			assigned = true
			lag, ok := lagByPartition[topicPartition{topic: assignment.Topic, partition: partition}]
			if !ok || lag < 0 {
				return "-"
			}
			total += lag
		}
	}
	if !assigned {
		return "-"
	}
	return fmt.Sprint(total)
}

func formatAssignments(assignments []kafka.GroupMemberTopic) string {
	if len(assignments) == 0 {
		return "-"
	}
	items := append([]kafka.GroupMemberTopic(nil), assignments...)
	sort.Slice(items, func(i, j int) bool { return items[i].Topic < items[j].Topic })

	parts := make([]string, 0, len(items))
	for _, item := range items {
		partitions := append([]int(nil), item.Partitions...)
		sort.Ints(partitions)
		partitionText := make([]string, 0, len(partitions))
		for _, partition := range partitions {
			partitionText = append(partitionText, fmt.Sprint(partition))
		}
		parts = append(parts, fmt.Sprintf("%s:[%s]", item.Topic, strings.Join(partitionText, ",")))
	}
	return strings.Join(parts, ";")
}

func fetchMetadata(broker string, topics ...string) (*kafka.MetadataResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	metadata, err := newClient(broker).Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("fetch metadata from broker %s: %w", broker, err)
	}
	return metadata, nil
}

func newClient(broker string) *kafka.Client {
	return &kafka.Client{
		Addr:    kafka.TCP(broker),
		Timeout: defaultTimeout,
	}
}

func newTableWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
}
