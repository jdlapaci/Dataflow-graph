package graph

import (
	"log"
	"fmt"
)

// Node is a concurrent processing unit
type Node struct {
	Name     string
	IsSource bool           // is it a source node?
	IsDrain  bool           // is it a drain node?
	Inputs   map[string]int // the inputs information map[node_name](data quantity)
	Outputs  map[string]int // the outputs information map[node_name](data quantity)
}

// Method on node object to start processing
func (n *Node) Run() {
	log.Printf("Node (%s): Initiated\n", n.Name)

	// For source node, block on source channel and wait for triggering
	if n.IsSource {
		<-sourceChannel
		log.Printf("Node (%s): ----- Start processing data ----- \n", n.Name)
		for node_name,data:=range n.Outputs {
			channel_name := fmt.Sprintf("%s-%s",n.Name,node_name)
			log.Printf("Node (%s): Send <%d> to (%s)\n", n.Name, data, node_name)
			channels[channel_name] <-Message{Quantity: data}
		}
	// For drain node, once finished, trigger the drain channel
	}else if n.IsDrain {
		for node_name,data:=range n.Inputs {
			channel_name := fmt.Sprintf("%s-%s",node_name,n.Name)
			for received_data:=0;received_data<data; {
				msg:= <-channels[channel_name]
				log.Printf("Node (%s): Receive <%d> from (%s)\n", n.Name, msg.Quantity, node_name)
				received_data+=msg.Quantity
			}
		}
		drainChannel <-Message{}
	// All nodes need to block on input channels,
	// start processing once it collects enough inputs,
	// then sends messages to output channels
	}else {
		var received_data int
		for node_name,data:=range n.Inputs {
			channel_name := fmt.Sprintf("%s-%s",node_name,n.Name)
			for received_data=0;received_data<data; {
				msg:= <-channels[channel_name]
				log.Printf("Node (%s): Receive <%d> from (%s)\n", n.Name, msg.Quantity, node_name)
				received_data+=msg.Quantity
			}
		}
		log.Printf("Node (%s): ----- Start processing data ----- \n", n.Name)
		for node_name,data:=range n.Outputs {
			channel_name := fmt.Sprintf("%s-%s",n.Name,node_name)
			for i:=received_data;i>0; {
				log.Printf("Node (%s): Send <%d> to (%s)\n", n.Name, data, node_name)
				channels[channel_name] <-Message{Quantity: data}
				i-=data
			}
		}
	}
}
