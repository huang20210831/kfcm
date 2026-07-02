package main

import (
	"fmt"
	"sort"

	"github.com/segmentio/kafka-go"
)

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
