package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/segmentio/kafka-go"
)

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
