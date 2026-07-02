package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

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
