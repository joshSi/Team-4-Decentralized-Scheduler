"""
Metrics collector for comparing centralized vs gossip schedulers.

Collects and analyzes:
- Resource utilization distribution
- Queue depth distribution
- Time to First Token (TTFT)
- Recovery speed after node failures
- Request latency
- Success rates
"""

import csv
import time
import statistics
from typing import List, Dict, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
import docker


@dataclass
class RequestMetrics:
    """Metrics for a single request."""
    request_id: str
    timestamp: float
    model_required: str
    scheduled_worker: str
    action: str  # SERVE, COLD_START, MIGRATE
    ttft: float = 0.0  # Time to first token (ms)
    total_latency: float = 0.0  # Total request latency (ms)
    success: bool = False
    error: str = ""


@dataclass
class WorkerMetrics:
    """Snapshot of worker metrics at a point in time."""
    timestamp: float
    worker_id: str
    memory_utilization: float
    queue_depth: int
    loaded_models: List[str] = field(default_factory=list)
    is_ready: bool = True


@dataclass
class ExperimentMetrics:
    """Aggregated metrics for an entire experiment."""
    experiment_name: str
    scheduler_type: str  # "centralized" or "gossip"
    start_time: float
    end_time: float
    duration: float

    # Request metrics
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    # Latency metrics (ms)
    avg_ttft: float = 0.0
    p50_ttft: float = 0.0
    p95_ttft: float = 0.0
    p99_ttft: float = 0.0
    avg_latency: float = 0.0
    p50_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0

    # Resource utilization
    avg_memory_utilization: float = 0.0
    min_memory_utilization: float = 0.0
    max_memory_utilization: float = 0.0
    p50_memory_utilization: float = 0.0
    p95_memory_utilization: float = 0.0

    # Queue depth
    avg_queue_depth: float = 0.0
    min_queue_depth: float = 0.0
    max_queue_depth: float = 0.0
    p50_queue_depth: float = 0.0
    p95_queue_depth: float = 0.0

    # Recovery metrics
    avg_recovery_time: float = 0.0  # Time to recover after node failure (s)

    # Action distribution
    serve_count: int = 0
    cold_start_count: int = 0
    migrate_count: int = 0


class MetricsCollector:
    """Collects metrics during experiment execution."""

    def __init__(self, experiment_name: str, scheduler_type: str):
        self.experiment_name = experiment_name
        self.scheduler_type = scheduler_type
        self.start_time = time.time()

        self.request_metrics: List[RequestMetrics] = []
        self.worker_metrics: List[WorkerMetrics] = []
        self.recovery_times: List[float] = []

        self.docker_client = None
        try:
            self.docker_client = docker.from_env()
        except:
            print("Warning: Docker client not available for metrics collection")

    def record_request(self, metrics: RequestMetrics):
        """Record metrics for a single request."""
        self.request_metrics.append(metrics)

    def record_worker_snapshot(self, metrics: WorkerMetrics):
        """Record a snapshot of worker state."""
        self.worker_metrics.append(metrics)

    def record_recovery_time(self, recovery_time: float):
        """Record time taken to recover from node failure."""
        self.recovery_times.append(recovery_time)

    def collect_docker_metrics(self, container_prefix: str):
        """Collect metrics from running Docker containers."""
        if not self.docker_client:
            return

        try:
            containers = self.docker_client.containers.list()
            timestamp = time.time()

            for container in containers:
                if container.name.startswith(container_prefix):
                    # Parse container logs or stats
                    # This is a placeholder - actual implementation depends on log format
                    pass
        except Exception as e:
            print(f"Error collecting Docker metrics: {e}")

    def compute_metrics(self) -> ExperimentMetrics:
        """Compute aggregated metrics from collected data."""
        end_time = time.time()

        # Request metrics
        total_requests = len(self.request_metrics)
        successful = [r for r in self.request_metrics if r.success]
        failed = [r for r in self.request_metrics if not r.success]

        # TTFT metrics
        ttfts = [r.ttft for r in successful if r.ttft > 0]
        latencies = [r.total_latency for r in successful if r.total_latency > 0]

        # Resource metrics
        memory_utils = [w.memory_utilization for w in self.worker_metrics]
        queue_depths = [w.queue_depth for w in self.worker_metrics]

        # Action counts
        serve_count = sum(1 for r in self.request_metrics if r.action == "SERVE" or r.action == "serve")
        cold_start_count = sum(1 for r in self.request_metrics if r.action == "COLD_START" or r.action == "cold_start")
        migrate_count = sum(1 for r in self.request_metrics if r.action == "MIGRATE" or r.action == "migrate")

        def percentile(data: List[float], p: float) -> float:
            if not data:
                return 0.0
            sorted_data = sorted(data)
            index = int(len(sorted_data) * p / 100)
            return sorted_data[min(index, len(sorted_data) - 1)]

        return ExperimentMetrics(
            experiment_name=self.experiment_name,
            scheduler_type=self.scheduler_type,
            start_time=self.start_time,
            end_time=end_time,
            duration=end_time - self.start_time,

            total_requests=total_requests,
            successful_requests=len(successful),
            failed_requests=len(failed),

            avg_ttft=statistics.mean(ttfts) if ttfts else 0.0,
            p50_ttft=percentile(ttfts, 50) if ttfts else 0.0,
            p95_ttft=percentile(ttfts, 95) if ttfts else 0.0,
            p99_ttft=percentile(ttfts, 99) if ttfts else 0.0,

            avg_latency=statistics.mean(latencies) if latencies else 0.0,
            p50_latency=percentile(latencies, 50) if latencies else 0.0,
            p95_latency=percentile(latencies, 95) if latencies else 0.0,
            p99_latency=percentile(latencies, 99) if latencies else 0.0,

            avg_memory_utilization=statistics.mean(memory_utils) if memory_utils else 0.0,
            min_memory_utilization=min(memory_utils) if memory_utils else 0.0,
            max_memory_utilization=max(memory_utils) if memory_utils else 0.0,
            p50_memory_utilization=percentile(memory_utils, 50) if memory_utils else 0.0,
            p95_memory_utilization=percentile(memory_utils, 95) if memory_utils else 0.0,

            avg_queue_depth=statistics.mean(queue_depths) if queue_depths else 0.0,
            min_queue_depth=min(queue_depths) if queue_depths else 0.0,
            max_queue_depth=max(queue_depths) if queue_depths else 0.0,
            p50_queue_depth=percentile(queue_depths, 50) if queue_depths else 0.0,
            p95_queue_depth=percentile(queue_depths, 50) if queue_depths else 0.0,

            avg_recovery_time=statistics.mean(self.recovery_times) if self.recovery_times else 0.0,

            serve_count=serve_count,
            cold_start_count=cold_start_count,
            migrate_count=migrate_count
        )

    def save_to_csv(self, output_dir: str = "results"):
        """Save metrics to CSV file."""
        metrics = self.compute_metrics()

        timestamp = datetime.fromtimestamp(self.start_time).strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/{self.experiment_name}_{timestamp}.csv"

        # Write summary metrics
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=asdict(metrics).keys())
            writer.writeheader()
            writer.writerow(asdict(metrics))

        print(f"Metrics saved to: {filename}")
        return filename

    def print_summary(self):
        """Print a human-readable summary of metrics."""
        metrics = self.compute_metrics()

        print("\n" + "=" * 80)
        print(f"EXPERIMENT RESULTS: {metrics.experiment_name}")
        print("=" * 80)
        print(f"Scheduler Type: {metrics.scheduler_type}")
        print(f"Duration: {metrics.duration:.2f}s")
        print(f"\nRequests:")
        print(f"  Total: {metrics.total_requests}")
        print(f"  Successful: {metrics.successful_requests} ({metrics.successful_requests/max(metrics.total_requests,1)*100:.1f}%)")
        print(f"  Failed: {metrics.failed_requests}")
        print(f"\nLatency (ms):")
        print(f"  Avg TTFT: {metrics.avg_ttft:.2f}")
        print(f"  P50 TTFT: {metrics.p50_ttft:.2f}")
        print(f"  P95 TTFT: {metrics.p95_ttft:.2f}")
        print(f"  P99 TTFT: {metrics.p99_ttft:.2f}")
        print(f"  Avg Total: {metrics.avg_latency:.2f}")
        print(f"\nResource Utilization:")
        print(f"  Avg Memory: {metrics.avg_memory_utilization:.1%}")
        print(f"  Min Memory: {metrics.min_memory_utilization:.1%}")
        print(f"  Max Memory: {metrics.max_memory_utilization:.1%}")
        print(f"  P50 Memory: {metrics.p50_memory_utilization:.1%}")
        print(f"  P95 Memory: {metrics.p95_memory_utilization:.1%}")
        print(f"\nQueue Depth:")
        print(f"  Avg: {metrics.avg_queue_depth:.2f}")
        print(f"  Min: {metrics.min_queue_depth}")
        print(f"  Max: {metrics.max_queue_depth}")
        print(f"  P50: {metrics.p50_queue_depth:.2f}")
        print(f"  P95: {metrics.p95_queue_depth:.2f}")
        print(f"\nAction Distribution:")
        print(f"  SERVE: {metrics.serve_count}")
        print(f"  COLD_START: {metrics.cold_start_count}")
        print(f"  MIGRATE: {metrics.migrate_count}")
        if metrics.avg_recovery_time > 0:
            print(f"\nRecovery:")
            print(f"  Avg Recovery Time: {metrics.avg_recovery_time:.2f}s")
        print("=" * 80 + "\n")
