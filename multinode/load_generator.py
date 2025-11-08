"""
Load generator for sending inference requests to the decentralized cluster.
"""

import time
import logging
import sys
import argparse
from multinode.dataset_loader import DatasetLoader
from multinode.workload_generator import WorkloadGenerator
from multinode.client import ClusterClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
logger = logging.getLogger("LoadGenerator")


class LoadGenerator:
    def __init__(
        self,
        entry_node_host: str = "127.0.0.1",
        entry_node_port: int = 9000,
        target_rps: float = 10.0,
        cv: float = 8.0,
        duration: float = 300.0,
        gsm8k_path: str = None,
        sharegpt_path: str = None
    ):
        self.target_rps = target_rps
        self.cv = cv
        self.duration = duration

        logger.info(f"Initializing load generator: RPS={target_rps}, CV={cv}, Duration={duration}s")
        logger.info(f"Cluster Entry Node: {entry_node_host}:{entry_node_port}")

        self.dataset_loader = DatasetLoader(max_tokens=2048, samples_per_dataset=4000)
        self.dataset_loader.load_gsm8k(gsm8k_path)
        self.dataset_loader.load_sharegpt(sharegpt_path)
        self.mixed_workload = self.dataset_loader.create_mixed_workload()

        self.workload_generator = WorkloadGenerator(
            target_rps=target_rps, cv=cv, duration=duration, models=['opt-1.3b', 'opt-2.7b'], seed=42
        )
        self.trace = self.workload_generator.generate_trace(dataset_samples=self.mixed_workload)

        self.client = ClusterClient(entry_nodes=[(entry_node_host, entry_node_port)], request_timeout=2.0)

        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0

    def wait_for_ready_workers(self, timeout: int = 300):
        logger.info(f"Waiting for cluster entry node to be ready (timeout: {timeout}s)...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.client.request_schedule(
                    request_id="readiness-check", model_required="opt-1.3b"
                )
                if response and hasattr(response, 'worker_id'):
                    logger.info(f"Cluster is ready! Entry node responded: {response.worker_id}")
                    return True
            except Exception as e:
                logger.debug(f"Readiness check failed: {e}")
            logger.info("Waiting for cluster to respond...")
            time.sleep(5)
        logger.error(f"Timeout: Cluster not ready after {timeout}s")
        return False

    def run(self):
        logger.info("=" * 70)
        logger.info("STARTING LOAD GENERATION")
        logger.info("=" * 70)

        if not self.wait_for_ready_workers(timeout=120):
            logger.error("Cluster not responding. Aborting load generation.")
            return

        logger.info(f"Sending {len(self.trace)} requests over {self.duration}s...")
        start_time = time.time()

        for req in self.trace:
            current_time = time.time() - start_time
            wait_time = req.timestamp - current_time
            if wait_time > 0:
                time.sleep(wait_time)
            if time.time() - start_time > self.duration:
                logger.info("Duration exceeded, stopping load generation")
                break

            response = self.client.request_schedule(request_id=req.request_id, model_required=req.model_id)
            self.total_requests += 1

            if response:
                self.successful_requests += 1
                if response.action.value == "serve":
                    self.cache_hits += 1
                elif response.action.value == "cold_start":
                    self.cache_misses += 1
                if self.total_requests % 100 == 0:
                    logger.info(
                        f"Req {self.total_requests}: {req.request_id} ({req.model_id}) → "
                        f"{response.worker_id} ({response.action.value})"
                    )
            else:
                self.failed_requests += 1
                if self.failed_requests % 10 == 0:
                    logger.warning(f"Failed request count: {self.failed_requests}")

        elapsed = time.time() - start_time
        self.print_statistics(elapsed)

    def print_statistics(self, elapsed: float):
        logger.info("")
        logger.info("=" * 70)
        logger.info("LOAD GENERATION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Duration: {elapsed:.2f}s")
        logger.info(f"Total requests: {self.total_requests}")
        logger.info(f"Successful: {self.successful_requests}")
        logger.info(f"Failed: {self.failed_requests}")
        if self.total_requests > 0:
            logger.info(f"Success rate: {100 * self.successful_requests / self.total_requests:.2f}%")
            logger.info(f"Actual RPS: {self.total_requests / elapsed:.2f}")
        if (self.cache_hits + self.cache_misses) > 0:
            logger.info(f"Cache hits: {self.cache_hits}")
            logger.info(f"Cache misses: {self.cache_misses}")
            logger.info(f"Cache hit rate: {100 * self.cache_hits / (self.cache_hits + self.cache_misses):.2f}%")
        logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Load Generator for Decentralized Cluster")
    parser.add_argument("--entry-host", default="127.0.0.1")
    parser.add_argument("--entry-port", type=int, default=9000)
    parser.add_argument("--rps", type=float, default=10.0)
    parser.add_argument("--cv", type=float, default=8.0)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument("--gsm8k", type=str)
    parser.add_argument("--sharegpt", type=str)
    args = parser.parse_args()

    generator = LoadGenerator(
        entry_node_host=args.entry_host,
        entry_node_port=args.entry_port,
        target_rps=args.rps,
        cv=args.cv,
        duration=args.duration,
        gsm8k_path=args.gsm8k,
        sharegpt_path=args.sharegpt
    )

    try:
        generator.run()
    except KeyboardInterrupt:
        logger.info("\nLoad generation interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
