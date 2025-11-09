# Team-4-Decentralized-Scheduler

## Usage Example

# Start 3 nodes in a gossip cluster
```
python3 decentralized_node.py --node-id node1 --port 9000 --preload-models opt-1.3b --verbose
python3 decentralized_node.py --node-id node2 --port 9001 --seed-nodes 127.0.0.1:9000 --preload-models opt-2.7b --verbose
python3 decentralized_node.py --node-id node3 --port 9002 --seed-nodes 127.0.0.1:9000 --verbose
```

# Send requests to any node
```
python3 load_generator.py --entry-host 127.0.0.1 --entry-port 9000 --rps 10
```