"""
System metrics collector for monitoring node performance.

Tracks CPU, GPU, memory, disk, network, and process-level metrics.
Designed for experimental analysis and performance profiling.
"""

import psutil
import time
import threading
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


@dataclass
class SystemMetrics:
    """Snapshot of system metrics at a point in time."""
    timestamp: float
    node_id: str
    
    # CPU metrics
    cpu_percent: float  # Overall CPU usage
    cpu_percent_per_core: List[float]  # Per-core CPU usage
    cpu_count: int
    cpu_freq_current: float  # MHz
    
    # Memory metrics
    memory_total: int  # bytes
    memory_available: int  # bytes
    memory_used: int  # bytes
    memory_percent: float
    
    # Process-specific metrics
    process_cpu_percent: float
    process_memory_mb: float
    process_memory_percent: float
    process_threads: int
    
    # Disk metrics
    disk_usage_percent: float
    disk_read_mb: float  # Cumulative MB read
    disk_write_mb: float  # Cumulative MB written
    
    # Network metrics
    network_sent_mb: float  # Cumulative MB sent
    network_recv_mb: float  # Cumulative MB received
    
    # GPU metrics (if available)
    gpu_available: bool
    gpu_utilization: Optional[float] = None  # percent
    gpu_memory_used: Optional[float] = None  # MB
    gpu_memory_total: Optional[float] = None  # MB
    gpu_memory_percent: Optional[float] = None
    gpu_temperature: Optional[float] = None  # Celsius
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


class MetricsCollector:
    """
    Collects system and process metrics periodically.
    
    Supports:
    - CPU usage (overall and per-core)
    - Memory usage (system and process)
    - Disk I/O
    - Network I/O
    - GPU metrics (NVIDIA via pynvml)
    - Process-specific metrics
    """
    
    def __init__(
        self,
        node_id: str,
        collection_interval: float = 1.0,
        enable_gpu: bool = True,
        log_to_file: Optional[str] = None,
        verbose: bool = False
    ):
        """
        Initialize metrics collector.
        
        Args:
            node_id: Unique identifier for this node
            collection_interval: Seconds between metric collections
            enable_gpu: Try to collect GPU metrics
            log_to_file: Optional file path to log metrics as JSON
            verbose: Enable detailed logging
        """
        self.node_id = node_id
        self.collection_interval = collection_interval
        self.enable_gpu = enable_gpu
        self.log_to_file = log_to_file
        self.verbose = verbose
        
        # Initialize logger first
        self.logger = logging.getLogger(f"MetricsCollector-{node_id}")
        
        # Process handle
        self.process = psutil.Process()
        
        # GPU support
        self.gpu_available = False
        self.nvml_initialized = False
        if enable_gpu:
            self._initialize_gpu()
        
        # Collection state
        self.is_collecting = False
        self._collection_thread = None
        
        # Metrics history
        self.metrics_history: List[SystemMetrics] = []
        self.history_lock = threading.Lock()
        
        # Baseline counters for rate calculations
        self._last_disk_io = None
        self._last_net_io = None
        self._last_collection_time = None
        
        if self.verbose:
            self.logger.info(f"Metrics collector initialized for {node_id}")
    
    def _initialize_gpu(self):
        """Initialize GPU monitoring (NVIDIA only)."""
        try:
            import pynvml
            pynvml.nvmlInit()
            self.pynvml = pynvml
            self.gpu_handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            self.gpu_available = True
            self.nvml_initialized = True
            self.logger.info("GPU monitoring enabled (NVIDIA)")
        except ImportError:
            self.logger.info("pynvml not installed. GPU metrics disabled. Install: pip install nvidia-ml-py3")
        except Exception as e:
            # This catches NVML library not found, no GPU, etc.
            self.logger.info(f"GPU monitoring not available: {e}")
    
    def _collect_cpu_metrics(self) -> Dict[str, Any]:
        """Collect CPU metrics."""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        cpu_percent_per_core = psutil.cpu_percent(interval=0.1, percpu=True)
        cpu_count = psutil.cpu_count()
        
        # CPU frequency
        cpu_freq = psutil.cpu_freq()
        cpu_freq_current = cpu_freq.current if cpu_freq else 0.0
        
        return {
            'cpu_percent': cpu_percent,
            'cpu_percent_per_core': cpu_percent_per_core,
            'cpu_count': cpu_count,
            'cpu_freq_current': cpu_freq_current
        }
    
    def _collect_memory_metrics(self) -> Dict[str, Any]:
        """Collect memory metrics."""
        mem = psutil.virtual_memory()
        
        return {
            'memory_total': mem.total,
            'memory_available': mem.available,
            'memory_used': mem.used,
            'memory_percent': mem.percent
        }
    
    def _collect_process_metrics(self) -> Dict[str, Any]:
        """Collect process-specific metrics."""
        try:
            # CPU percent (relative to single core)
            process_cpu = self.process.cpu_percent(interval=0.1)
            
            # Memory
            mem_info = self.process.memory_info()
            process_memory_mb = mem_info.rss / (1024 * 1024)
            process_memory_percent = self.process.memory_percent()
            
            # Threads
            process_threads = self.process.num_threads()
            
            return {
                'process_cpu_percent': process_cpu,
                'process_memory_mb': process_memory_mb,
                'process_memory_percent': process_memory_percent,
                'process_threads': process_threads
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            self.logger.warning(f"Failed to collect process metrics: {e}")
            return {
                'process_cpu_percent': 0.0,
                'process_memory_mb': 0.0,
                'process_memory_percent': 0.0,
                'process_threads': 0
            }
    
    def _collect_disk_metrics(self) -> Dict[str, Any]:
        """Collect disk I/O metrics."""
        disk_usage = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()
        
        if disk_io:
            disk_read_mb = disk_io.read_bytes / (1024 * 1024)
            disk_write_mb = disk_io.write_bytes / (1024 * 1024)
        else:
            disk_read_mb = 0.0
            disk_write_mb = 0.0
        
        return {
            'disk_usage_percent': disk_usage.percent,
            'disk_read_mb': disk_read_mb,
            'disk_write_mb': disk_write_mb
        }
    
    def _collect_network_metrics(self) -> Dict[str, Any]:
        """Collect network I/O metrics."""
        net_io = psutil.net_io_counters()
        
        if net_io:
            network_sent_mb = net_io.bytes_sent / (1024 * 1024)
            network_recv_mb = net_io.bytes_recv / (1024 * 1024)
        else:
            network_sent_mb = 0.0
            network_recv_mb = 0.0
        
        return {
            'network_sent_mb': network_sent_mb,
            'network_recv_mb': network_recv_mb
        }
    
    def _collect_gpu_metrics(self) -> Dict[str, Any]:
        """Collect GPU metrics (NVIDIA only)."""
        if not self.gpu_available or not self.nvml_initialized:
            return {
                'gpu_available': False,
                'gpu_utilization': None,
                'gpu_memory_used': None,
                'gpu_memory_total': None,
                'gpu_memory_percent': None,
                'gpu_temperature': None
            }
        
        try:
            # Utilization
            util = self.pynvml.nvmlDeviceGetUtilizationRates(self.gpu_handle)
            gpu_utilization = util.gpu
            
            # Memory
            mem_info = self.pynvml.nvmlDeviceGetMemoryInfo(self.gpu_handle)
            gpu_memory_used = mem_info.used / (1024 * 1024)  # MB
            gpu_memory_total = mem_info.total / (1024 * 1024)  # MB
            gpu_memory_percent = (mem_info.used / mem_info.total) * 100
            
            # Temperature
            try:
                gpu_temperature = self.pynvml.nvmlDeviceGetTemperature(
                    self.gpu_handle,
                    self.pynvml.NVML_TEMPERATURE_GPU
                )
            except:
                gpu_temperature = None
            
            return {
                'gpu_available': True,
                'gpu_utilization': float(gpu_utilization),
                'gpu_memory_used': float(gpu_memory_used),
                'gpu_memory_total': float(gpu_memory_total),
                'gpu_memory_percent': float(gpu_memory_percent),
                'gpu_temperature': float(gpu_temperature) if gpu_temperature else None
            }
        
        except Exception as e:
            self.logger.warning(f"Failed to collect GPU metrics: {e}")
            return {
                'gpu_available': False,
                'gpu_utilization': None,
                'gpu_memory_used': None,
                'gpu_memory_total': None,
                'gpu_memory_percent': None,
                'gpu_temperature': None
            }
    
    def collect_metrics(self) -> SystemMetrics:
        """
        Collect all metrics and return a snapshot.
        
        Returns:
            SystemMetrics object with current measurements
        """
        timestamp = time.time()
        
        # Collect all metrics
        cpu_metrics = self._collect_cpu_metrics()
        memory_metrics = self._collect_memory_metrics()
        process_metrics = self._collect_process_metrics()
        disk_metrics = self._collect_disk_metrics()
        network_metrics = self._collect_network_metrics()
        gpu_metrics = self._collect_gpu_metrics()
        
        # Combine into SystemMetrics
        metrics = SystemMetrics(
            timestamp=timestamp,
            node_id=self.node_id,
            **cpu_metrics,
            **memory_metrics,
            **process_metrics,
            **disk_metrics,
            **network_metrics,
            **gpu_metrics
        )
        
        return metrics
    
    def _collection_loop(self):
        """Background thread for periodic metric collection."""
        self.logger.info(f"Metrics collection started for {self.node_id}")
        
        while self.is_collecting:
            try:
                # Collect metrics
                metrics = self.collect_metrics()
                
                # Store in history
                with self.history_lock:
                    self.metrics_history.append(metrics)
                
                # Log to file if configured
                if self.log_to_file:
                    self._log_to_file(metrics)
                
                # Print metrics if verbose
                if self.verbose:
                    self._print_metrics(metrics)
                
            except Exception as e:
                self.logger.error(f"Error collecting metrics: {e}", exc_info=True)
            
            # Sleep until next collection
            time.sleep(self.collection_interval)
        
        self.logger.info(f"Metrics collection stopped for {self.node_id}")
    
    def _log_to_file(self, metrics: SystemMetrics):
        """Log metrics to file in JSON format."""
        try:
            with open(self.log_to_file, 'a') as f:
                f.write(metrics.to_json() + '\n')
        except Exception as e:
            self.logger.error(f"Failed to log metrics to file: {e}")
    
    def _print_metrics(self, metrics: SystemMetrics):
        """Print formatted metrics."""
        print(f"\n--- Metrics for {self.node_id} at {datetime.fromtimestamp(metrics.timestamp)} ---")
        print(f"CPU: {metrics.cpu_percent:.1f}% (process: {metrics.process_cpu_percent:.1f}%)")
        print(f"Memory: {metrics.memory_percent:.1f}% (process: {metrics.process_memory_mb:.1f} MB)")
        print(f"Disk: {metrics.disk_usage_percent:.1f}%")
        print(f"Network: Sent {metrics.network_sent_mb:.2f} MB, Recv {metrics.network_recv_mb:.2f} MB")
        if metrics.gpu_available:
            print(f"GPU: {metrics.gpu_utilization:.1f}% util, {metrics.gpu_memory_percent:.1f}% mem")
    
    def start(self):
        """Start periodic metrics collection."""
        if self.is_collecting:
            self.logger.warning("Metrics collection already running")
            return
        
        self.is_collecting = True
        self._collection_thread = threading.Thread(
            target=self._collection_loop,
            name=f"{self.node_id}-Metrics"
        )
        self._collection_thread.start()
        self.logger.info(f"Started metrics collection for {self.node_id}")
    
    def stop(self):
        """Stop metrics collection."""
        if not self.is_collecting:
            return
        
        self.is_collecting = False
        
        if self._collection_thread:
            self._collection_thread.join()
        
        # Cleanup GPU
        if self.nvml_initialized:
            try:
                self.pynvml.nvmlShutdown()
            except:
                pass
        
        self.logger.info(f"Stopped metrics collection for {self.node_id}")
    
    def get_metrics_history(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> List[SystemMetrics]:
        """
        Get metrics history within time range.
        
        Args:
            start_time: Start timestamp (inclusive)
            end_time: End timestamp (inclusive)
            
        Returns:
            List of SystemMetrics
        """
        with self.history_lock:
            filtered = self.metrics_history
            
            if start_time:
                filtered = [m for m in filtered if m.timestamp >= start_time]
            if end_time:
                filtered = [m for m in filtered if m.timestamp <= end_time]
            
            return filtered.copy()
    
    def get_summary_stats(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Get summary statistics over time range.
        
        Args:
            start_time: Start timestamp
            end_time: End timestamp
            
        Returns:
            Dictionary with summary statistics
        """
        metrics = self.get_metrics_history(start_time, end_time)
        
        if not metrics:
            return {}
        
        # Calculate averages
        avg_cpu = sum(m.cpu_percent for m in metrics) / len(metrics)
        avg_memory = sum(m.memory_percent for m in metrics) / len(metrics)
        avg_process_cpu = sum(m.process_cpu_percent for m in metrics) / len(metrics)
        avg_process_memory = sum(m.process_memory_mb for m in metrics) / len(metrics)
        
        # Peak values
        peak_cpu = max(m.cpu_percent for m in metrics)
        peak_memory = max(m.memory_percent for m in metrics)
        peak_process_cpu = max(m.process_cpu_percent for m in metrics)
        peak_process_memory = max(m.process_memory_mb for m in metrics)
        
        # Network/Disk totals
        total_net_sent = metrics[-1].network_sent_mb - metrics[0].network_sent_mb
        total_net_recv = metrics[-1].network_recv_mb - metrics[0].network_recv_mb
        total_disk_read = metrics[-1].disk_read_mb - metrics[0].disk_read_mb
        total_disk_write = metrics[-1].disk_write_mb - metrics[0].disk_write_mb
        
        stats = {
            'node_id': self.node_id,
            'num_samples': len(metrics),
            'duration_seconds': metrics[-1].timestamp - metrics[0].timestamp,
            'avg_cpu_percent': avg_cpu,
            'peak_cpu_percent': peak_cpu,
            'avg_memory_percent': avg_memory,
            'peak_memory_percent': peak_memory,
            'avg_process_cpu_percent': avg_process_cpu,
            'peak_process_cpu_percent': peak_process_cpu,
            'avg_process_memory_mb': avg_process_memory,
            'peak_process_memory_mb': peak_process_memory,
            'total_network_sent_mb': total_net_sent,
            'total_network_recv_mb': total_net_recv,
            'total_disk_read_mb': total_disk_read,
            'total_disk_write_mb': total_disk_write
        }
        
        # GPU stats if available
        if metrics[0].gpu_available:
            gpu_utils = [m.gpu_utilization for m in metrics if m.gpu_utilization is not None]
            gpu_mems = [m.gpu_memory_percent for m in metrics if m.gpu_memory_percent is not None]
            
            if gpu_utils:
                stats['avg_gpu_utilization'] = sum(gpu_utils) / len(gpu_utils)
                stats['peak_gpu_utilization'] = max(gpu_utils)
            
            if gpu_mems:
                stats['avg_gpu_memory_percent'] = sum(gpu_mems) / len(gpu_mems)
                stats['peak_gpu_memory_percent'] = max(gpu_mems)
        
        return stats
    
    def clear_history(self):
        """Clear metrics history."""
        with self.history_lock:
            self.metrics_history.clear()
        self.logger.info("Metrics history cleared")
    
    def export_to_csv(self, filepath: str):
        """
        Export metrics history to CSV file.
        
        Args:
            filepath: Path to CSV file
        """
        import csv
        
        with self.history_lock:
            if not self.metrics_history:
                self.logger.warning("No metrics to export")
                return
            
            # Get field names from first metric
            fieldnames = list(self.metrics_history[0].to_dict().keys())
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for metrics in self.metrics_history:
                    writer.writerow(metrics.to_dict())
            
            self.logger.info(f"Exported {len(self.metrics_history)} metrics to {filepath}")


def main():
    """Example usage of MetricsCollector."""
    import argparse
    import signal
    
    parser = argparse.ArgumentParser(description="System Metrics Collector")
    parser.add_argument("--node-id", default="test-node", help="Node identifier")
    parser.add_argument("--interval", type=float, default=1.0, help="Collection interval (seconds)")
    parser.add_argument("--duration", type=float, default=60.0, help="Collection duration (seconds)")
    parser.add_argument("--log-file", type=str, help="Log metrics to file (JSON)")
    parser.add_argument("--csv-file", type=str, help="Export to CSV file")
    parser.add_argument("--disable-gpu", action="store_true", help="Disable GPU monitoring")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    collector = MetricsCollector(
        node_id=args.node_id,
        collection_interval=args.interval,
        enable_gpu=not args.disable_gpu,
        log_to_file=args.log_file,
        verbose=args.verbose
    )
    
    def signal_handler(sig, frame):
        print("\n\nStopping metrics collection...")
        collector.stop()
        
        # Print summary
        stats = collector.get_summary_stats()
        print("\n=== Summary Statistics ===")
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2f}")
            else:
                print(f"{key}: {value}")
        
        # Export if requested
        if args.csv_file:
            collector.export_to_csv(args.csv_file)
            print(f"\nExported metrics to {args.csv_file}")
        
        exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start collection
    collector.start()
    print(f"Collecting metrics for {args.node_id}...")
    print(f"Press Ctrl+C to stop")
    
    # Run for specified duration
    time.sleep(args.duration)
    signal_handler(None, None)


if __name__ == "__main__":
    main()
