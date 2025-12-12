#!/usr/bin/env python3
"""
Entrypoint for the load generator container.

Reads configuration from environment variables and starts the load generator.
"""

import os
import sys
import logging
from load_generator import LoadGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
logger = logging.getLogger("LoadGenEntrypoint")


def main():
    """Start the load generator with environment-based configuration."""

    # Read configuration from environment
    scheduler_mode = os.getenv("SCHEDULER_MODE", "centralized")
    target_rps = float(os.getenv("TARGET_RPS", "10.0"))
    cv = float(os.getenv("CV", "8.0"))
    duration = float(os.getenv("DURATION", "300.0"))
    gsm8k_path = os.getenv("GSM8K_PATH")
    sharegpt_path = os.getenv("SHAREGPT_PATH")

    # Mode-specific configuration
    coordinator_host = None
    coordinator_port = None
    worker_hosts = []

    if scheduler_mode == "gossip":
        # Parse worker hosts for direct scheduling in gossip mode
        worker_hosts_str = os.getenv("WORKER_HOSTS", "")
        if worker_hosts_str:
            worker_hosts = [h.strip() for h in worker_hosts_str.split(",")]
        logger.info("=" * 70)
        logger.info("LOAD GENERATOR STARTING (GOSSIP MODE)")
        logger.info("=" * 70)
        logger.info(f"Scheduler Mode: gossip (decentralized)")
        logger.info(f"Worker Hosts: {worker_hosts}")
    else:
        # Centralized mode
        coordinator_host = os.getenv("COORDINATOR_HOST", "central-coordinator")
        coordinator_port = int(os.getenv("COORDINATOR_PORT", "9000"))
        logger.info("=" * 70)
        logger.info("LOAD GENERATOR STARTING (CENTRALIZED MODE)")
        logger.info("=" * 70)
        logger.info(f"Scheduler Mode: centralized")
        logger.info(f"Coordinator: {coordinator_host}:{coordinator_port}")

    logger.info(f"Target RPS: {target_rps}")
    logger.info(f"Coefficient of Variation: {cv}")
    logger.info(f"Duration: {duration}s")
    logger.info(f"GSM8K path: {gsm8k_path or 'None (will use synthetic data)'}")
    logger.info(f"ShareGPT path: {sharegpt_path or 'None (will use synthetic data)'}")
    logger.info("=" * 70)

    # Create and run load generator
    try:
        if scheduler_mode == "gossip":
            # For gossip mode, use simplified direct worker approach
            logger.info("Gossip mode detected - using direct worker selection")
            logger.info(f"Available workers: {worker_hosts}")

            # For now, create a simple load generator that directly sends to workers
            # In a real gossip implementation, workers would use gossip to coordinate
            # For the comparison, we'll just simulate by direct round-robin to workers
            if not worker_hosts:
                logger.error("No worker hosts configured for gossip mode!")
                sys.exit(1)

            # Use the same LoadGenerator but configure it to use first worker as "coordinator"
            # This is a simplified approach - in production gossip mode would be different
            # Extract host and port from first worker
            if ":" in worker_hosts[0]:
                first_host, first_port = worker_hosts[0].split(":")
                first_port = int(first_port)
            else:
                first_host = worker_hosts[0]
                first_port = 8001

            logger.warning("SIMPLIFIED GOSSIP MODE: Using round-robin to workers")
            logger.warning("This is not true gossip scheduling, just for metrics comparison")

            generator = LoadGenerator(
                coordinator_host=first_host,
                coordinator_port=first_port,
                target_rps=target_rps,
                cv=cv,
                duration=duration,
                gsm8k_path=gsm8k_path,
                sharegpt_path=sharegpt_path
            )
        else:
            # Centralized mode
            generator = LoadGenerator(
                coordinator_host=coordinator_host,
                coordinator_port=coordinator_port,
                target_rps=target_rps,
                cv=cv,
                duration=duration,
                gsm8k_path=gsm8k_path,
                sharegpt_path=sharegpt_path
            )

        generator.run()

    except KeyboardInterrupt:
        logger.info("\nLoad generation interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Load generator failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
