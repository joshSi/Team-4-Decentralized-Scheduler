#!/usr/bin/env python3
import os
import sys
import signal
import logging
from multinode.node import DecentralizedNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("NodeEntrypoint")

def parse_seed_nodes(raw: str):
    # "host1:port1 host2:port2"
    seeds = []
    if not raw:
        return seeds
    for token in raw.split():
        if ":" not in token:
            continue
        h, p = token.split(":", 1)
        try:
            seeds.append((h, int(p)))
        except ValueError:
            pass
    return seeds

def main():
    node_id = os.getenv("NODE_ID", "n0")
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "9000"))
    seed_nodes = parse_seed_nodes(os.getenv("SEED_NODES", ""))
    gossip_interval = float(os.getenv("GOSSIP_INTERVAL", "1.0"))
    verbose = os.getenv("VERBOSE", "false").lower() == "true"

    preload = os.getenv("PRELOAD_MODELS", "").split()
    log.info(f"Starting node {node_id} on {host}:{port}")
    log.info(f"Seeds: {seed_nodes}, preload: {preload}")

    node = DecentralizedNode(
        node_id=node_id,
        host=host,
        port=port,
        seed_nodes=seed_nodes,
        gossip_interval=gossip_interval,
        verbose=verbose,
        use_real_models=False,  # set True if wiring up GCS/HF device paths
    )

    def _shutdown(signum, _):
        log.info(f"Signal {signum} received, shutting down...")
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    if preload:
        node.initialize(preload)
    node.start()

    # Keep alive
    signal.pause()

if __name__ == "__main__":
    main()
