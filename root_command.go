package main

import "github.com/spf13/cobra"

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
func showHelp(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}
