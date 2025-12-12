# Team-4-Decentralized-Scheduler

## Usage Example

# Start 3 nodes in a gossip cluster
```
python3 decentralized_node.py --node-id node0 --host 0.0.0.0 --port 9000 --use-real-models --device cpu --preload-models opt-125m --metrics-csv metric.csv

python3 decentralized_node.py --node-id node1 --host 0.0.0.0 --port 9001 --device cpu --preload-models opt-125m --seed-nodes 0.0.0.0:9000 --use-real-models --metrics-csv metric.csv

python3 decentralized_node.py --node-id node1 --host 0.0.0.0 --port 9002 --device cpu --preload-models opt-350m --seed-nodes 0.0.0.0:9000 --use-real-models --metrics-csv metric.csv
```

# Send requests to any node
```
python3 load_generator.py --entry-host 10.128.0.9 --entry-port 9000 --rps 10
```