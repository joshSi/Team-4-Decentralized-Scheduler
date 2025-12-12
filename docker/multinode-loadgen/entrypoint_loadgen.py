#!/usr/bin/env python3
import os
import sys
import logging
from multinode.load_generator import LoadGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("LoadGenEntrypoint")

def main():
    entry_host = os.getenv("ENTRY_HOST", "multinode-node-0")
    entry_port = int(os.getenv("ENTRY_PORT", "9000"))
    rps = float(os.getenv("RPS", "5.0"))
    cv = float(os.getenv("CV", "8.0"))
    duration = float(os.getenv("DURATION", "60.0"))
    gsm8k = os.getenv("GSM8K_PATH", None)
    sharegpt = os.getenv("SHAREGPT_PATH", None)

    lg = LoadGenerator(
        entry_node_host=entry_host,
        entry_node_port=entry_port,
        target_rps=rps,
        cv=cv,
        duration=duration,
        gsm8k_path=gsm8k,
        sharegpt_path=sharegpt,
    )

    try:
        lg.run()
    except KeyboardInterrupt:
        log.info("Interrupted")

if __name__ == "__main__":
    main()
