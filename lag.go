package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"sort"
)

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
