package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"sort"
)

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
