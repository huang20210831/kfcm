package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"sort"
	"strings"
)

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
