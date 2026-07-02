package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
)

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
func newTableWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
}
