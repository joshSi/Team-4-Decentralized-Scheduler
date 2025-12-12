#!/usr/bin/env python3
"""
Compare centralized vs gossip schedulers.

This script runs both scheduler types with identical workloads and compares:
- Resource utilization
- Queue depth
- TTFT (Time to First Token)
- Recovery speed after node failures
- Request latency
- Success rates
"""

import subprocess
import time
import docker
import argparse
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from metrics_collector import MetricsCollector, RequestMetrics, WorkerMetrics


class SchedulerComparison:
    """Manages comparison experiments between schedulers."""

    def __init__(self, duration: int = 300, rps: float = 5.0, cv: float = 8.0):
        self.duration = duration
        self.rps = rps
        self.cv = cv
        self.docker_client = docker.from_env()
        self.results_dir = Path(__file__).parent.parent / "results"
        self.results_dir.mkdir(exist_ok=True)

    def run_centralized_experiment(self, experiment_name: str) -> MetricsCollector:
        """Run centralized scheduler experiment."""
        print("\n" + "=" * 80)
        print("RUNNING CENTRALIZED SCHEDULER EXPERIMENT")
        print("=" * 80)

        collector = MetricsCollector(experiment_name, "centralized")

        # Start centralized scheduler
        compose_dir = Path(__file__).parent.parent / "config" / "docker"
        os.chdir(compose_dir)

        print("Starting centralized scheduler...")
        subprocess.run([
            "docker-compose", "up", "-d"
        ], check=True)

        # Wait for initialization
        print("Waiting for workers to load models...")
        time.sleep(60)

        # Collect metrics during experiment
        start_time = time.time()
        while time.time() - start_time < self.duration:
            self._collect_centralized_metrics(collector)
            time.sleep(5)

        # Stop containers
        print("\nStopping centralized scheduler...")
        subprocess.run([
            "docker-compose", "down"
        ], check=True)

        return collector

    def run_gossip_experiment(self, experiment_name: str) -> MetricsCollector:
        """Run gossip scheduler experiment."""
        print("\n" + "=" * 80)
        print("RUNNING GOSSIP SCHEDULER EXPERIMENT")
        print("=" * 80)

        collector = MetricsCollector(experiment_name, "gossip")

        # Start gossip scheduler
        compose_dir = Path(__file__).parent.parent / "config" / "docker"
        os.chdir(compose_dir)

        print("Starting gossip scheduler...")
        subprocess.run([
            "docker-compose", "-f", "docker-compose-gossip.yml", "up", "-d"
        ], check=True)

        # Wait for initialization
        print("Waiting for workers to load models...")
        time.sleep(60)

        # Collect metrics during experiment
        start_time = time.time()
        while time.time() - start_time < self.duration:
            self._collect_gossip_metrics(collector)
            time.sleep(5)

        # Stop containers
        print("\nStopping gossip scheduler...")
        subprocess.run([
            "docker-compose", "-f", "docker-compose-gossip.yml", "down"
        ], check=True)

        return collector

    def _collect_centralized_metrics(self, collector: MetricsCollector):
        """Collect metrics from centralized scheduler containers."""
        try:
            # Get load generator logs for request metrics
            load_gen = self.docker_client.containers.get("load-generator")
            logs = load_gen.logs(tail=100).decode('utf-8')

            # Parse logs for request metrics
            for line in logs.split('\n'):
                if "Schedule response" in line:
                    # Parse schedule response
                    # Example: Schedule response for req-0: worker=worker-1, action=serve
                    try:
                        parts = line.split("Schedule response for ")
                        if len(parts) > 1:
                            req_info = parts[1]
                            req_id = req_info.split(":")[0].strip()
                            worker = ""
                            action = ""
                            if "worker=" in req_info:
                                worker = req_info.split("worker=")[1].split(",")[0].strip()
                            if "action=" in req_info:
                                action = req_info.split("action=")[1].strip()

                            metrics = RequestMetrics(
                                request_id=req_id,
                                timestamp=time.time(),
                                model_required="facebook/opt-125m",
                                scheduled_worker=worker,
                                action=action,
                                success=True
                            )
                            collector.record_request(metrics)
                    except Exception as e:
                        print(f"Error parsing log line: {e}")

            # Get worker metrics
            for i in [1, 2, 3]:
                try:
                    worker = self.docker_client.containers.get(f"worker-{i}")
                    logs = worker.logs(tail=50).decode('utf-8')

                    # Parse for memory and queue info
                    memory_util = 0.0
                    queue_depth = 0
                    is_ready = False

                    for line in logs.split('\n'):
                        if "memory utilization" in line.lower():
                            try:
                                # Extract memory percentage
                                parts = line.split(":")
                                if len(parts) > 1:
                                    mem_str = parts[-1].strip().replace("%", "")
                                    memory_util = float(mem_str) / 100.0
                            except:
                                pass
                        if "Worker ready" in line:
                            is_ready = "True" in line

                    worker_metrics = WorkerMetrics(
                        timestamp=time.time(),
                        worker_id=f"worker-{i}",
                        memory_utilization=memory_util,
                        queue_depth=queue_depth,
                        loaded_models=["facebook/opt-125m"],
                        is_ready=is_ready
                    )
                    collector.record_worker_snapshot(worker_metrics)
                except Exception as e:
                    print(f"Error collecting worker-{i} metrics: {e}")

        except Exception as e:
            print(f"Error collecting centralized metrics: {e}")

    def _collect_gossip_metrics(self, collector: MetricsCollector):
        """Collect metrics from gossip scheduler containers."""
        try:
            # Get load generator logs for request metrics
            load_gen = self.docker_client.containers.get("gossip-load-generator")
            logs = load_gen.logs(tail=100).decode('utf-8')

            # Parse logs for request metrics
            for line in logs.split('\n'):
                if "Schedule response" in line or "Inference" in line:
                    # Parse request response
                    try:
                        parts = line.split("Schedule response for ")
                        if len(parts) > 1:
                            req_info = parts[1]
                            req_id = req_info.split(":")[0].strip()
                            worker = ""
                            action = ""
                            if "worker=" in req_info:
                                worker = req_info.split("worker=")[1].split(",")[0].strip()
                            if "action=" in req_info:
                                action = req_info.split("action=")[1].strip()

                            metrics = RequestMetrics(
                                request_id=req_id,
                                timestamp=time.time(),
                                model_required="facebook/opt-125m",
                                scheduled_worker=worker,
                                action=action,
                                success=True
                            )
                            collector.record_request(metrics)
                    except Exception as e:
                        print(f"Error parsing log line: {e}")

            # Get worker metrics from gossip workers
            for i in [1, 2, 3]:
                try:
                    worker = self.docker_client.containers.get(f"gossip-worker-{i}")
                    logs = worker.logs(tail=50).decode('utf-8')

                    # Parse for memory and queue info
                    memory_util = 0.0
                    queue_depth = 0
                    is_ready = False

                    for line in logs.split('\n'):
                        if "memory utilization" in line.lower():
                            try:
                                # Extract memory percentage
                                parts = line.split(":")
                                if len(parts) > 1:
                                    mem_str = parts[-1].strip().replace("%", "")
                                    memory_util = float(mem_str) / 100.0
                            except:
                                pass
                        if "Worker ready" in line:
                            is_ready = "True" in line
                        if "queue" in line.lower() and "depth" in line.lower():
                            try:
                                # Extract queue depth
                                parts = line.split(":")
                                if len(parts) > 1:
                                    queue_depth = int(parts[-1].strip())
                            except:
                                pass

                    worker_metrics = WorkerMetrics(
                        timestamp=time.time(),
                        worker_id=f"gossip-worker-{i}",
                        memory_utilization=memory_util,
                        queue_depth=queue_depth,
                        loaded_models=["facebook/opt-125m"],
                        is_ready=is_ready
                    )
                    collector.record_worker_snapshot(worker_metrics)
                except Exception as e:
                    print(f"Error collecting gossip-worker-{i} metrics: {e}")

        except Exception as e:
            print(f"Error collecting gossip metrics: {e}")

    def run_comparison(self, experiment_base_name: str):
        """Run both experiments and generate comparison."""
        print("\n" + "=" * 80)
        print("SCHEDULER COMPARISON EXPERIMENT")
        print("=" * 80)
        print(f"Duration: {self.duration}s")
        print(f"Target RPS: {self.rps}")
        print(f"CV: {self.cv}")
        print("=" * 80)

        # Run centralized
        centralized_name = f"{experiment_base_name}_centralized"
        centralized_collector = self.run_centralized_experiment(centralized_name)

        # Wait between experiments
        print("\nWaiting 30s between experiments...")
        time.sleep(30)

        # Run gossip
        gossip_name = f"{experiment_base_name}_gossip"
        gossip_collector = self.run_gossip_experiment(gossip_name)

        # Print summaries
        print("\n" + "=" * 80)
        print("CENTRALIZED SCHEDULER RESULTS")
        print("=" * 80)
        centralized_collector.print_summary()
        centralized_file = centralized_collector.save_to_csv(str(self.results_dir))

        print("\n" + "=" * 80)
        print("GOSSIP SCHEDULER RESULTS")
        print("=" * 80)
        gossip_collector.print_summary()
        gossip_file = gossip_collector.save_to_csv(str(self.results_dir))

        # Generate comparison
        self._generate_comparison_report(
            centralized_collector.compute_metrics(),
            gossip_collector.compute_metrics()
        )

        print(f"\nResults saved to:")
        print(f"  Centralized: {centralized_file}")
        print(f"  Gossip: {gossip_file}")

    def _generate_comparison_report(self, centralized, gossip):
        """Generate side-by-side comparison report."""
        print("\n" + "=" * 80)
        print("COMPARISON REPORT: CENTRALIZED VS GOSSIP")
        print("=" * 80)

        print(f"\n{'Metric':<40} {'Centralized':<20} {'Gossip':<20} {'Winner'}")
        print("-" * 80)

        def compare(metric_name, cent_val, gossip_val, lower_is_better=True):
            if lower_is_better:
                winner = "Centralized" if cent_val < gossip_val else "Gossip"
                if cent_val == gossip_val:
                    winner = "Tie"
            else:
                winner = "Centralized" if cent_val > gossip_val else "Gossip"
                if cent_val == gossip_val:
                    winner = "Tie"

            print(f"{metric_name:<40} {str(cent_val):<20} {str(gossip_val):<20} {winner}")

        compare("Success Rate (%)",
                f"{centralized.successful_requests/max(centralized.total_requests,1)*100:.1f}%",
                f"{gossip.successful_requests/max(gossip.total_requests,1)*100:.1f}%",
                lower_is_better=False)

        compare("Avg TTFT (ms)", f"{centralized.avg_ttft:.2f}", f"{gossip.avg_ttft:.2f}")
        compare("P95 TTFT (ms)", f"{centralized.p95_ttft:.2f}", f"{gossip.p95_ttft:.2f}")
        compare("P99 TTFT (ms)", f"{centralized.p99_ttft:.2f}", f"{gossip.p99_ttft:.2f}")

        compare("Avg Memory Util", f"{centralized.avg_memory_utilization:.1%}",
                f"{gossip.avg_memory_utilization:.1%}", lower_is_better=False)
        compare("P95 Memory Util", f"{centralized.p95_memory_utilization:.1%}",
                f"{gossip.p95_memory_utilization:.1%}", lower_is_better=False)

        compare("Avg Queue Depth", f"{centralized.avg_queue_depth:.2f}",
                f"{gossip.avg_queue_depth:.2f}")
        compare("P95 Queue Depth", f"{centralized.p95_queue_depth:.2f}",
                f"{gossip.p95_queue_depth:.2f}")

        if centralized.avg_recovery_time > 0 or gossip.avg_recovery_time > 0:
            compare("Avg Recovery Time (s)", f"{centralized.avg_recovery_time:.2f}",
                    f"{gossip.avg_recovery_time:.2f}")

        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Compare centralized vs gossip schedulers")
    parser.add_argument("--duration", type=int, default=300,
                        help="Experiment duration in seconds (default: 300)")
    parser.add_argument("--rps", type=float, default=5.0,
                        help="Target requests per second (default: 5.0)")
    parser.add_argument("--cv", type=float, default=8.0,
                        help="Coefficient of variation for burstiness (default: 8.0)")
    parser.add_argument("--name", type=str, default="scheduler_comparison",
                        help="Base name for experiment (default: scheduler_comparison)")
    parser.add_argument("--centralized-only", action="store_true",
                        help="Run only centralized scheduler")
    parser.add_argument("--gossip-only", action="store_true",
                        help="Run only gossip scheduler")

    args = parser.parse_args()

    comparison = SchedulerComparison(
        duration=args.duration,
        rps=args.rps,
        cv=args.cv
    )

    if args.centralized_only:
        collector = comparison.run_centralized_experiment(f"{args.name}_centralized")
        collector.print_summary()
        collector.save_to_csv(str(comparison.results_dir))
    elif args.gossip_only:
        collector = comparison.run_gossip_experiment(f"{args.name}_gossip")
        collector.print_summary()
        collector.save_to_csv(str(comparison.results_dir))
    else:
        comparison.run_comparison(args.name)


if __name__ == "__main__":
    main()
