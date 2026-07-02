package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
)

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
