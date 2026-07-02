package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
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
	cmd.AddCommand(newClusterCommand(), newDebugCommand())
	return cmd
}

func newClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage Kafka clusters",
		RunE:  showHelp,
	}
	cmd.AddCommand(newClusterAddCommand(), newClusterListCommand(), newClusterDeleteCommand())
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

func newClusterDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete Kafka cluster resources",
		RunE:  showHelp,
	}
	cmd.AddCommand(newDeleteTopicCommand(), newDeleteConsumerGroupCommand())
	return cmd
}

func newDeleteTopicCommand() *cobra.Command {
	var name string
	var yes bool

	cmd := &cobra.Command{
		Use:     "topic <broker-ip:port>",
		Aliases: []string{"topics"},
		Short:   "Delete a Kafka topic",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("--name is required")
			}
			if !yes {
				return fmt.Errorf("refusing to delete topic %q without --yes", name)
			}
			return deleteTopic(args[0], name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "topic name")
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm deletion")
	return cmd
}

func newDeleteConsumerGroupCommand() *cobra.Command {
	var name string
	var yes bool

	cmd := &cobra.Command{
		Use:     "consumergroup <broker-ip:port>",
		Aliases: []string{"consumergroups", "consumer-group", "consumer-groups"},
		Short:   "Delete a Kafka consumer group",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("--name is required")
			}
			if !yes {
				return fmt.Errorf("refusing to delete consumer group %q without --yes", name)
			}
			return deleteConsumerGroup(args[0], name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "consumer group name")
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm deletion")
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

func newDebugCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "Debug raw Kafka protocol data",
		RunE:  showHelp,
	}
	cmd.AddCommand(newDebugDescribeGroupCommand())
	return cmd
}

func newDebugDescribeGroupCommand() *cobra.Command {
	var name string

	cmd := &cobra.Command{
		Use:   "describegroup <broker-ip:port>",
		Short: "Print raw DescribeGroups member metadata and assignments",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("--name is required")
			}
			return debugDescribeGroup(newClient(args[0]), name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "consumer group name")
	return cmd
}

func showHelp(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

func deleteTopic(broker string, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	client := newClient(broker)

	resp, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{name}})
	if err != nil {
		return fmt.Errorf("delete topic %q: %w", name, err)
	}
	if topicErr := resp.Errors[name]; topicErr != nil {
		return fmt.Errorf("delete topic %q: %w", name, topicErr)
	}
	fmt.Printf("deleted topic %q\n", name)
	return nil
}

func deleteConsumerGroup(broker string, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	client := newClient(broker)

	resp, err := client.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{GroupIDs: []string{name}})
	if err != nil {
		return fmt.Errorf("delete consumer group %q: %w", name, err)
	}
	if groupErr := resp.Errors[name]; groupErr != nil {
		return fmt.Errorf("delete consumer group %q: %w", name, groupErr)
	}
	fmt.Printf("deleted consumer group %q\n", name)
	return nil
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
	groups, err := describeGroupsLenient(client, name)
	if err != nil {
		return fmt.Errorf("describe consumer group %q: %w", name, err)
	}
	if len(groups) == 0 {
		return fmt.Errorf("consumer group %q not found", name)
	}

	for _, g := range groups {
		if g.err != nil {
			return fmt.Errorf("describe consumer group %q: %w", name, g.err)
		}
		lagByPartition, err := fetchGroupLags(client, g.groupID, g.members)
		if err != nil {
			return err
		}

		fmt.Printf("GROUP\t%s\n", g.groupID)
		fmt.Printf("STATE\t%s\n", g.state)

		w := newTableWriter()
		fmt.Fprintln(w, "MEMBER_ID\tCLIENT_ID\tCLIENT_HOST\tASSIGNMENTS\tLAG")
		for _, m := range g.members {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", m.MemberID, m.ClientID, m.ClientHost, formatAssignments(m.MemberAssignments.Topics), memberLagText(m.MemberAssignments.Topics, lagByPartition))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

type consumerGroupDescription struct {
	groupID string
	state   string
	members []kafka.DescribeGroupsResponseMember
	err     error
}

func describeGroupsRaw(client *kafka.Client, groupID string) (*describegroups.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	transport := client.Transport
	if transport == nil {
		transport = kafka.DefaultTransport
	}
	resp, err := transport.RoundTrip(ctx, client.Addr, &describegroups.Request{Groups: []string{groupID}})
	if err != nil {
		return nil, err
	}
	apiResp, ok := resp.(*describegroups.Response)
	if !ok {
		return nil, fmt.Errorf("unexpected DescribeGroups response type %T", resp)
	}
	return apiResp, nil
}

func debugDescribeGroup(client *kafka.Client, groupID string) error {
	apiResp, err := describeGroupsRaw(client, groupID)
	if err != nil {
		return fmt.Errorf("describe group %q: %w", groupID, err)
	}
	if len(apiResp.Groups) == 0 {
		return fmt.Errorf("consumer group %q not found", groupID)
	}

	for _, group := range apiResp.Groups {
		fmt.Printf("GROUP_ID\t%s\n", group.GroupID)
		fmt.Printf("ERROR_CODE\t%d\n", group.ErrorCode)
		fmt.Printf("STATE\t%s\n", group.GroupState)
		fmt.Printf("PROTOCOL_TYPE\t%s\n", group.ProtocolType)
		fmt.Printf("PROTOCOL_DATA\t%s\n", group.ProtocolData)
		fmt.Printf("MEMBER_COUNT\t%d\n", len(group.Members))
		for i, member := range group.Members {
			assignments, assignmentErr := decodeAssignmentsLenient(member.MemberAssignment)
			fmt.Printf("\nMEMBER_INDEX\t%d\n", i)
			fmt.Printf("MEMBER_ID\t%s\n", member.MemberID)
			fmt.Printf("GROUP_INSTANCE_ID\t%s\n", member.GroupInstanceID)
			fmt.Printf("CLIENT_ID\t%s\n", member.ClientID)
			fmt.Printf("CLIENT_HOST\t%s\n", member.ClientHost)
			fmt.Printf("MEMBER_METADATA_LENGTH\t%d\n", len(member.MemberMetadata))
			fmt.Printf("MEMBER_METADATA_HEX\t%s\n", hex.EncodeToString(member.MemberMetadata))
			printParsedMemberMetadata(member.MemberMetadata)
			fmt.Printf("MEMBER_ASSIGNMENT_LENGTH\t%d\n", len(member.MemberAssignment))
			fmt.Printf("MEMBER_ASSIGNMENT_HEX\t%s\n", hex.EncodeToString(member.MemberAssignment))
			printParsedMemberAssignment(member.MemberAssignment)
			if assignmentErr != nil {
				fmt.Printf("PARSED_ASSIGNMENTS_ERROR\t%s\n", assignmentErr)
			} else {
				fmt.Printf("PARSED_ASSIGNMENTS\t%s\n", formatAssignments(assignments))
			}
		}
	}
	return nil
}

func describeGroupsLenient(client *kafka.Client, groupID string) ([]consumerGroupDescription, error) {
	apiResp, err := describeGroupsRaw(client, groupID)
	if err != nil {
		return nil, err
	}
	groups := make([]consumerGroupDescription, 0, len(apiResp.Groups))
	for _, apiGroup := range apiResp.Groups {
		group := consumerGroupDescription{
			groupID: apiGroup.GroupID,
			state:   apiGroup.GroupState,
		}
		if apiGroup.ErrorCode != 0 {
			group.err = kafka.Error(apiGroup.ErrorCode)
		}
		for _, member := range apiGroup.Members {
			assignments, err := decodeAssignmentsLenient(member.MemberAssignment)
			if err != nil {
				return nil, fmt.Errorf("decode member assignment for %s: %w", member.MemberID, err)
			}
			group.members = append(group.members, kafka.DescribeGroupsResponseMember{
				MemberID:          member.MemberID,
				ClientID:          member.ClientID,
				ClientHost:        member.ClientHost,
				MemberAssignments: kafka.DescribeGroupsResponseAssignments{Topics: assignments},
			})
		}
		groups = append(groups, group)
	}
	return groups, nil
}

type memberMetadataDebug struct {
	version             int16
	topics              []string
	userData            []byte
	ownedPartitions     []kafka.GroupMemberTopic
	remainingBytes      []byte
	remainingAfterBasic []byte
}

type memberAssignmentDebug struct {
	version        int16
	assignments    []kafka.GroupMemberTopic
	userData       []byte
	remainingBytes []byte
}

func printParsedMemberMetadata(raw []byte) {
	metadata, err := parseMemberMetadataDebug(raw)
	if err != nil {
		fmt.Printf("MEMBER_METADATA_PARSE_ERROR\t%s\n", err)
		return
	}
	fmt.Printf("MEMBER_METADATA_VERSION\t%d\n", metadata.version)
	fmt.Printf("MEMBER_METADATA_TOPICS\t%s\n", strings.Join(metadata.topics, ","))
	fmt.Printf("MEMBER_METADATA_USER_DATA_LENGTH\t%d\n", len(metadata.userData))
	fmt.Printf("MEMBER_METADATA_USER_DATA_HEX\t%s\n", hex.EncodeToString(metadata.userData))
	fmt.Printf("MEMBER_METADATA_OWNED_PARTITIONS\t%s\n", formatAssignments(metadata.ownedPartitions))
	fmt.Printf("MEMBER_METADATA_REMAINING_LENGTH\t%d\n", len(metadata.remainingBytes))
	fmt.Printf("MEMBER_METADATA_REMAINING_HEX\t%s\n", hex.EncodeToString(metadata.remainingBytes))
	fmt.Printf("MEMBER_METADATA_REMAINING_AFTER_BASIC_LENGTH\t%d\n", len(metadata.remainingAfterBasic))
	fmt.Printf("MEMBER_METADATA_REMAINING_AFTER_BASIC_HEX\t%s\n", hex.EncodeToString(metadata.remainingAfterBasic))
}

func printParsedMemberAssignment(raw []byte) {
	assignment, err := parseMemberAssignmentDebug(raw)
	if err != nil {
		fmt.Printf("MEMBER_ASSIGNMENT_PARSE_ERROR\t%s\n", err)
		return
	}
	fmt.Printf("MEMBER_ASSIGNMENT_VERSION\t%d\n", assignment.version)
	fmt.Printf("MEMBER_ASSIGNMENT_PARTITIONS\t%s\n", formatAssignments(assignment.assignments))
	fmt.Printf("MEMBER_ASSIGNMENT_USER_DATA_LENGTH\t%d\n", len(assignment.userData))
	fmt.Printf("MEMBER_ASSIGNMENT_USER_DATA_HEX\t%s\n", hex.EncodeToString(assignment.userData))
	fmt.Printf("MEMBER_ASSIGNMENT_REMAINING_LENGTH\t%d\n", len(assignment.remainingBytes))
	fmt.Printf("MEMBER_ASSIGNMENT_REMAINING_HEX\t%s\n", hex.EncodeToString(assignment.remainingBytes))
}

func parseMemberMetadataDebug(raw []byte) (memberMetadataDebug, error) {
	var metadata memberMetadataDebug
	if len(raw) == 0 {
		return metadata, nil
	}
	r := bytes.NewReader(raw)
	version, err := readInt16Value(r)
	if err != nil {
		return metadata, err
	}
	metadata.version = version
	topics, err := readStringArrayValue(r)
	if err != nil {
		return metadata, err
	}
	metadata.topics = topics
	userData, err := readBytesValue(r)
	if err != nil {
		return metadata, err
	}
	metadata.userData = userData
	metadata.remainingAfterBasic = remainingBytes(r)
	if metadata.version == 1 && r.Len() > 0 {
		ownedPartitions, err := readTopicPartitionsArrayValue(r)
		if err != nil {
			return metadata, err
		}
		metadata.ownedPartitions = ownedPartitions
	}
	metadata.remainingBytes = remainingBytes(r)
	return metadata, nil
}

func parseMemberAssignmentDebug(raw []byte) (memberAssignmentDebug, error) {
	var assignment memberAssignmentDebug
	if len(raw) == 0 {
		return assignment, nil
	}
	r := bytes.NewReader(raw)
	version, err := readInt16Value(r)
	if err != nil {
		return assignment, err
	}
	assignment.version = version
	assignments, err := readTopicPartitionsArrayValue(r)
	if err != nil {
		return assignment, err
	}
	assignment.assignments = assignments
	userData, err := readBytesValue(r)
	if err != nil {
		return assignment, err
	}
	assignment.userData = userData
	assignment.remainingBytes = remainingBytes(r)
	return assignment, nil
}

func readStringArrayValue(r *bytes.Reader) ([]string, error) {
	count, err := readInt32Value(r)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	if count > 100000 {
		return nil, fmt.Errorf("invalid string array count %d", count)
	}
	items := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		item, err := readStringValue(r)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func readBytesValue(r *bytes.Reader) ([]byte, error) {
	length, err := readInt32Value(r)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if int64(length) > int64(r.Len()) {
		return nil, fmt.Errorf("invalid bytes length %d", length)
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	return buf, err
}

func readTopicPartitionsArrayValue(r *bytes.Reader) ([]kafka.GroupMemberTopic, error) {
	count, err := readInt32Value(r)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	if count > 100000 {
		return nil, fmt.Errorf("invalid topic partition array count %d", count)
	}
	items := make([]kafka.GroupMemberTopic, 0, count)
	for i := int32(0); i < count; i++ {
		topic, err := readStringValue(r)
		if err != nil {
			return nil, err
		}
		partitions, err := readInt32ArrayValue(r)
		if err != nil {
			return nil, err
		}
		items = append(items, kafka.GroupMemberTopic{Topic: topic, Partitions: partitions})
	}
	return items, nil
}

func readInt32ArrayValue(r *bytes.Reader) ([]int, error) {
	count, err := readInt32Value(r)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	if count > 1000000 {
		return nil, fmt.Errorf("invalid int32 array count %d", count)
	}
	items := make([]int, 0, count)
	for i := int32(0); i < count; i++ {
		item, err := readInt32Value(r)
		if err != nil {
			return nil, err
		}
		items = append(items, int(item))
	}
	return items, nil
}

func remainingBytes(r *bytes.Reader) []byte {
	if r.Len() == 0 {
		return nil
	}
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil
	}
	buf := make([]byte, r.Len())
	_, _ = io.ReadFull(r, buf)
	_, _ = r.Seek(pos, io.SeekStart)
	return buf
}

func decodeAssignmentsLenient(raw []byte) ([]kafka.GroupMemberTopic, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	r := bytes.NewReader(raw)
	if _, err := readInt16Value(r); err != nil {
		return nil, err
	}
	count, err := readInt32Value(r)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	if count > 100000 {
		return nil, fmt.Errorf("invalid assignment topic count %d", count)
	}
	assignments := make([]kafka.GroupMemberTopic, 0, count)
	for i := int32(0); i < count; i++ {
		topic, err := readStringValue(r)
		if err != nil {
			return nil, err
		}
		partitionCount, err := readInt32Value(r)
		if err != nil {
			return nil, err
		}
		if partitionCount < 0 {
			continue
		}
		if partitionCount > 1000000 {
			return nil, fmt.Errorf("invalid assignment partition count %d", partitionCount)
		}
		item := kafka.GroupMemberTopic{Topic: topic, Partitions: make([]int, 0, partitionCount)}
		for j := int32(0); j < partitionCount; j++ {
			partition, err := readInt32Value(r)
			if err != nil {
				return nil, err
			}
			item.Partitions = append(item.Partitions, int(partition))
		}
		assignments = append(assignments, item)
	}
	// Ignore remaining bytes: they are user data or newer protocol extensions.
	return assignments, nil
}

func readInt16Value(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func readInt32Value(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func readStringValue(r *bytes.Reader) (string, error) {
	length, err := readInt16Value(r)
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil
	}
	if int64(length) > int64(r.Len()) {
		return "", fmt.Errorf("invalid string length %d", length)
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
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
