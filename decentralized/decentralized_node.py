"""
Decentralized node for peer-to-peer scheduling and work.
...
"""

import socket
import threading
import pickle
import time
import random
import logging
from typing import List, Optional, Dict, Tuple, Any
from contracts import (
    WorkerLoadReport,
    ScheduleRequest,
    ScheduleResponse,
    PlacementAction
)
from model_loader import ModelLoader
from system_metrics import MetricsCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(threadName)s] %(message)s')


class DecentralizedNode:
    """
    A single node in the decentralized, gossip-based cluster.
    Acts as both a worker and a scheduler.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        seed_nodes: List[Tuple[str, int]],
        gossip_interval: float = 1.0,
        worker_timeout: float = 10.0,
        verbose: bool = True,
        use_real_models: bool = False,
        gcs_bucket: str = "remote_model",
        cache_dir: str = "/tmp/model_cache",
        device: str = "cpu",
        enable_metrics: bool = True,
        enable_gpu_metrics: bool = False,
        metrics_interval: float = 1.0,
        metrics_log_file: Optional[str] = None
    ):
        """
        Initialize a decentralized node.
        ...
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr = (host, port)
        self.seed_nodes = seed_nodes
        self.gossip_interval = gossip_interval
        self.worker_timeout = worker_timeout
        self.verbose = verbose
        self.use_real_models = use_real_models

        # --- Worker State (from WorkerNode) ---
        self.queue_depth: int = 0
        self.memory_utilization: float = 0.0
        self.is_ready: bool = False
        self.initialization_time: float = 0.0

        # Model loader
        if use_real_models:
            self.model_loader = ModelLoader(
                gcs_bucket=gcs_bucket,
                cache_dir=cache_dir,
                device=device
            )
        else:
            self.model_loader = None
            self._simulated_models: List[str] = []

        # --- Scheduler State (from CentralCoordinator) ---
        # Global state: {worker_id: (WorkerLoadReport, last_update_time, (host, port))}
        self.cluster_state: Dict[str, Tuple[WorkerLoadReport, float, Tuple[str, int]]] = {}
        self.state_lock = threading.RLock()

        # Statistics
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0

        # Metrics collection
        self.metrics_collector = None
        if enable_metrics:
            self.metrics_collector = MetricsCollector(
                node_id=node_id,
                collection_interval=metrics_interval,
                enable_gpu=enable_gpu_metrics,
                log_to_file=metrics_log_file,
                verbose=False
            )

        # --- Networking & Threading ---
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.is_running = True
        self.is_paused = False  # <-- NEW: Flag to simulate crash/partition
        self._listener_thread = None
        self._gossip_thread = None

        self.logger = logging.getLogger(f"Node-{node_id}")
        if self.verbose:
            self.logger.info(
                f"Node {node_id} initializing at {host}:{port}. "
                f"Gossip interval: {gossip_interval}s. "
                f"Real models: {use_real_models}"
            )

    # --- Helper Methods ---

    def _send_message(self, message: Dict, addr: Tuple[str, int]):
        """Serialize and send a UDP message."""
        try:
            serialized = pickle.dumps(message)
            self.sock.sendto(serialized, addr)
        except (socket.error, pickle.PicklingError) as e:
            self.logger.warning(f"Failed to send message to {addr}: {e}")

    def _get_random_peer(self) -> Optional[Tuple[str, int]]:
        """
        Get a random peer from the cluster state or seed nodes (excluding self).
        """
        with self.state_lock:
            # Get addresses from all known peers (excluding self)
            peer_addresses = {
                addr for wid, (_, _, addr) in self.cluster_state.items()
                if wid != self.node_id
            }

            # Add all seed nodes to this set of potential targets.
            # Using a set automatically handles any duplicates.
            all_possible_peers = peer_addresses.union(self.seed_nodes)

            # Remove self address to prevent gossiping to self,
            # which could happen if listed as a seed node.
            all_possible_peers.discard(self.addr)

            if all_possible_peers:
                # At least one valid peer, pick one at random
                return random.choice(list(all_possible_peers))
            else:
                # Isolated node (no peers, and seed list was empty or only contained self)
                return None
            
    def _create_own_report(self) -> WorkerLoadReport:
        """Helper to generate a load report for this node."""
        with self.state_lock:
            # Update memory utilization from metrics if available
            if self.metrics_collector:
                try:
                    metrics = self.metrics_collector.collect_metrics()
                    self.memory_utilization = metrics.process_memory_percent / 100.0
                except:
                    pass
            elif self.use_real_models and self.is_ready:
                self.memory_utilization = self._get_memory_utilization()

            return WorkerLoadReport(
                node_id=self.node_id,
                models_loaded=self.get_loaded_models(),
                queue_depth=self.queue_depth,
                memory_utilization=self.memory_utilization,
                is_ready=self.is_ready,
                timestamp=time.time()
            )

    # --- Core Threads (Gossip & Listener) ---

    def _listener(self):
        """Listen for all incoming UDP messages."""
        if self.verbose:
            self.logger.info(f"{self.node_id} listener started")

        while self.is_running:
            try:
                if self.is_paused:
                    time.sleep(0.1)  # Prevent busy-looping
                    continue

                raw_data, addr = self.sock.recvfrom(8192)
                message = pickle.loads(raw_data)
                msg_type = message.get('type')

                if msg_type == 'schedule_request':
                    request = ScheduleRequest.from_dict(message['payload'])
                    if self.verbose:
                        self.logger.debug(f"Received schedule_request for {request.model_required} from {addr}")
                    
                    # Make scheduling decision locally
                    try:
                        response = self.schedule(request)
                        payload = response.to_dict()
                        msg = {'type': 'schedule_response', 'payload': payload}
                    except Exception as e:
                        self.logger.error(f"Scheduling failed: {e}")
                        msg = {'type': 'schedule_error', 'payload': {'error': str(e)}}
                    
                    self._send_message(msg, addr)

                elif msg_type == 'gossip_sync':
                    incoming_state = message['payload']
                    self._merge_cluster_state(incoming_state)

                else:
                    self.logger.debug(f"Received unknown message type: {msg_type}")

            except (socket.error, pickle.UnpicklingError, EOFError) as e:
                if self.is_running:
                    self.logger.error(f"{self.node_id} listener error: {e}", exc_info=True)
                break
        
        if self.verbose:
            self.logger.info(f"{self.node_id} listener stopped")

    def _gossip(self):
        """Periodically send our cluster state to a random peer."""
        if self.verbose:
            self.logger.info(f"{self.node_id} gossip thread started")
        
        while self.is_running:
            try:
                if self.is_paused:
                    time.sleep(self.gossip_interval)  # Wait for full interval
                    continue

                with self.state_lock:
                    # 1. Update our own state, including our address
                    report = self._create_own_report()
                    self.cluster_state[self.node_id] = (report, time.time(), self.addr)

                    # 2. Prune dead workers
                    self._cleanup_dead_workers()
                    
                    # 3. Select a peer to gossip to
                    target_addr = self._get_random_peer()

                    if not target_addr:
                        self.logger.debug("No peers or seeds to gossip to.")
                        continue
                    
                    if target_addr == self.addr:
                         self.logger.debug("Chose to gossip to self, skipping.")
                         continue
                    
                    # 4. Serialize state for gossiping
                    # Payload: {worker_id: (report_dict, timestamp, address)}
                    payload = {
                        wid: (report.to_dict(), ts, addr)
                        for wid, (report, ts, addr) in self.cluster_state.items()
                    }
                    message = {'type': 'gossip_sync', 'payload': payload}

                # 5. Send gossip
                self._send_message(message, target_addr)
                if self.verbose:
                    self.logger.debug(f"Gossiped state ({len(payload)} nodes) to {target_addr}")
                            
            except Exception as e:
                if self.is_running:
                    self.logger.error(f"Error in gossip thread: {e}", exc_info=True)

        if self.verbose:
            self.logger.info(f"{self.node_id} gossip thread stopped")

    def _merge_cluster_state(self, incoming_state: Dict[str, Tuple[Dict, float, Tuple[str, int]]]):
        """Merge an incoming cluster state with our local state."""
        with self.state_lock:
            merged_count = 0
            new_peers_found = []
            
            for worker_id, (report_dict, timestamp, addr) in incoming_state.items():
                if worker_id == self.node_id:
                    continue  # Ignore state about ourselves

                if (
                    worker_id not in self.cluster_state or
                    timestamp > self.cluster_state[worker_id][1]
                ):
                    if worker_id not in self.cluster_state:
                        new_peers_found.append(worker_id)
                        print(f"METRIC_PEER_DISCOVERY:{self.node_id},{worker_id},{time.time()}", flush=True)
                        
                    try:
                        report = WorkerLoadReport.from_dict(report_dict)
                        self.cluster_state[worker_id] = (report, timestamp, addr)
                        merged_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to deserialize report for {worker_id}: {e}")
            
            if self.verbose and new_peers_found:
                self.logger.info(f"Discovered {len(new_peers_found)} new peers: {new_peers_found}")
            elif self.verbose and merged_count > 0:
                self.logger.debug(f"Merged state for {merged_count} updated nodes.")

    def _cleanup_dead_workers(self):
        """Remove workers that have not reported in a while."""
        # This lock must be acquired by the caller (_gossip)
        current_time = time.time()
        dead_workers = [
            worker_id
            for worker_id, (_, last_seen, _) in self.cluster_state.items()
            if current_time - last_seen > self.worker_timeout and worker_id != self.node_id
        ]
        for worker_id in dead_workers:
            del self.cluster_state[worker_id]
            self.logger.warning(f"Removed dead worker: {worker_id}")
            print(f"METRIC_FAILURE_DETECTED:{self.node_id},{worker_id},{time.time()}", flush=True)


    def _get_active_workers(self) -> Dict[str, WorkerLoadReport]:
        """Get all active workers."""
        # This lock must be acquired by the caller
        current_time = time.time()
        return {
            worker_id: report
            for worker_id, (report, ts, _) in self.cluster_state.items()
            if (current_time - ts) <= self.worker_timeout or worker_id == self.node_id
        }

    # --- Scheduling Logic (from CentralCoordinator) ---

    def _calculate_worker_score(
        self,
        report: WorkerLoadReport,
        has_model: bool
    ) -> float:
        """Calculate a score for a worker (higher is better)."""
        if not report.is_ready:
            return -float('inf') # Never schedule on a node that isn't ready
            
        score = 1000.0 if has_model else 0.0
        score -= report.queue_depth * 10
        score -= report.memory_utilization * 100
        return score

    def schedule(self, request: ScheduleRequest) -> ScheduleResponse:
        """Find the optimal worker based on local cluster state."""
        self.total_requests += 1
        
        with self.state_lock:
            active_workers = self._get_active_workers()

            if not active_workers:
                self.logger.error(f"No active workers available for request {request.request_id}")
                raise RuntimeError("No active workers available")

            workers_with_model = []
            workers_without_model = []

            for worker_id, report in active_workers.items():
                has_model = request.model_required in report.models_loaded
                score = self._calculate_worker_score(report, has_model)
                
                if has_model:
                    workers_with_model.append((worker_id, report, score))
                else:
                    workers_without_model.append((worker_id, report, score))

            # Decision logic
            if workers_with_model:
                # Cache hit
                self.cache_hits += 1
                workers_with_model.sort(key=lambda x: x[2], reverse=True)
                best_worker_id, best_report, _ = workers_with_model[0]
                action = PlacementAction.SERVE
                reason = f"Worker {best_worker_id} has model. Queue: {best_report.queue_depth}"
                estimated_wait = best_report.queue_depth * 0.5 

            elif workers_without_model:
                # Cache miss: cold start
                self.cache_misses += 1
                workers_without_model.sort(key=lambda x: x[2], reverse=True)
                best_worker_id, best_report, _ = workers_without_model[0]
                action = PlacementAction.COLD_START
                reason = f"No worker has model. Cold starting on {best_worker_id}."
                estimated_wait = 10.0 + (best_report.queue_depth * 0.5)

            else:
                # This happens if all workers are not ready
                self.logger.error(f"No *ready* workers available for request {request.request_id}")
                raise RuntimeError("No ready workers available")

            if self.verbose:
                self.logger.info(
                    f"Scheduled {request.request_id} ({request.model_required}) "
                    f"to {best_worker_id} ({action.value})"
                )

            return ScheduleResponse(
                worker_id=best_worker_id,
                action=action,
                estimated_wait_time=estimated_wait,
                reason=reason
            )

    # --- Worker Logic (from WorkerNode) ---

    def get_loaded_models(self) -> List[str]:
        """Get list of currently loaded models."""
        if self.use_real_models:
            return self.model_loader.get_loaded_models()
        else:
            return self._simulated_models.copy()

    def load_model(self, model_id: str) -> bool:
        """Load a model (real or simulated)."""
        with self.state_lock:
            if self.use_real_models:
                success = self.model_loader.load_model(model_id)
                if success:
                    self.logger.info(f"Loaded real model: {model_id}")
                else:
                    self.logger.error(f"Failed to load real model: {model_id}")
                return success
            else:
                if model_id not in self._simulated_models:
                    self._simulated_models.append(model_id)
                    self.logger.info(f"Loaded simulated model: {model_id}")
                return True

    def unload_model(self, model_id: str) -> bool:
        """Unload a model (real or simulated)."""
        with self.state_lock:
            if self.use_real_models:
                success = self.model_loader.unload_model(model_id)
                if success:
                    self.logger.info(f"Unloaded real model: {model_id}")
                else:
                    self.logger.error(f"Failed to unload real model: {model_id}")
                return success
            else:
                if model_id in self._simulated_models:
                    self._simulated_models.remove(model_id)
                    self.logger.info(f"Unloaded simulated model: {model_id}")
                return True

    def _get_memory_utilization(self) -> float:
        """
        Get actual memory utilization of current process using psutil.

        Returns:
            Memory utilization as fraction (0.0 to 1.0)
        """
        try:
            import psutil
            process = psutil.Process()
            mem_percent = process.memory_percent()
            return mem_percent / 100.0
        except Exception as e:
            self.logger.warning(f"Failed to get memory utilization: {e}")
            return self.memory_utilization

    def set_queue_depth(self, depth: int):
        with self.state_lock:
            self.queue_depth = depth

    def simulate_workload_change(self):
        """Simulate random workload changes for testing."""
        if self.is_paused:
            return
            
        with self.state_lock:
            self.queue_depth = max(0, self.queue_depth + random.randint(-2, 3))
            self.memory_utilization = max(0.0, min(1.0, self.memory_utilization + random.uniform(-0.1, 0.1)))

    def initialize(self, models_to_load: List[str]) -> float:
        """Initialize worker by loading required models."""
        start_time = time.time()
        self.logger.info(f"{self.node_id} initializing with models: {models_to_load}")

        for model_id in models_to_load:
            self.logger.info(f"{self.node_id} loading model {model_id}...")
            self.load_model(model_id)

        self.initialization_time = time.time() - start_time
        self.is_ready = True  # Mark as ready *after* loading

        self.logger.info(
            f"{self.node_id} initialization complete in {self.initialization_time:.2f}s. "
            f"Ready to serve."
        )
        return self.initialization_time

    def pause(self):
        """Pause all gossip and processing, simulating a crash or partition."""
        self.logger.warning(f"--- NODE PAUSED (simulating crash/partition) ---")
        self.is_paused = True

    def resume(self):
        """Resume all gossip and processing."""
        self.logger.warning(f"--- NODE RESUMED (recovering from fault) ---")
        self.is_paused = False

    # --- Start/Stop ---

    def start(self):
        """Bind socket and start listener/gossip threads."""
        if not self.is_ready and self.use_real_models:
            self.logger.warning(
                f"{self.node_id} starting without initialization. "
                "Call initialize() first to pre-load models."
            )
            self.is_ready = True # Mark as ready even if no models loaded
        
        if not self.use_real_models:
            self.is_ready = True # Always ready in simulation mode

        self.sock.bind(self.addr)
        self.logger.info(f"Node {self.node_id} bound to {self.addr}")

        # Start metrics collection
        if self.metrics_collector:
            self.metrics_collector.start()
            self.logger.info(f"Started metrics collection for {self.node_id}")

        self._listener_thread = threading.Thread(
            target=self._listener,
            name=f"{self.node_id}-Listener"
        )
        self._gossip_thread = threading.Thread(
            target=self._gossip,
            name=f"{self.node_id}-Gossip"
        )

        self._listener_thread.start()
        self._gossip_thread.start()
        self.logger.info(f"{self.node_id} started (ready: {self.is_ready})")

    def stop(self):
        """Stop the node gracefully."""
        if not self.is_running:
            return

        self.is_running = False
        
        # Stop metrics collection
        if self.metrics_collector:
            self.metrics_collector.stop()
            
            # Print metrics summary
            if self.verbose:
                stats = self.metrics_collector.get_summary_stats()
                print(f"\n=== Metrics Summary for {self.node_id} ===")
                for key, value in stats.items():
                    if isinstance(value, float):
                        print(f"{key}: {value:.2f}")
                    else:
                        print(f"{key}: {value}")
        
        self.sock.close() # This will interrupt recvfrom in _listener

        if self._gossip_thread:
            self._gossip_thread.join()
        if self._listener_thread:
            self._listener_thread.join()

        if self.verbose:
            self.logger.info(f"{self.node_id} stopped")

    # --- Measurability ---
    
    def get_cluster_state(self) -> Dict:
        """Get a summary of the *local view* of the cluster state."""
        with self.state_lock:
            active_workers = self._get_active_workers()
            total_queue = sum(w.queue_depth for w in active_workers.values())
            avg_memory = (
                sum(w.memory_utilization for w in active_workers.values()) / len(active_workers)
                if active_workers else 0.0
            )
            ready_workers = sum(1 for w in active_workers.values() if w.is_ready)

            peer_data = {}
            for worker_id in active_workers.keys():
                # Check if key exists before accessing, as state might change
                if worker_id in self.cluster_state:
                    report, ts, addr = self.cluster_state[worker_id]
                    peer_data[worker_id] = (report.to_dict(), ts, addr)

            state = {
                "node_id": self.node_id,
                "num_active_peers": len(active_workers),
                "num_ready_peers": ready_workers,
                "total_requests_scheduled_by_me": self.total_requests,
                "cache_hit_rate_my_decisions": self.cache_hits / self.total_requests if self.total_requests > 0 else 0.0,
                "cluster_total_queue_depth": total_queue,
                "cluster_average_memory": avg_memory,
                "peers": peer_data
            }
            
            # Add current metrics if available
            if self.metrics_collector:
                try:
                    current_metrics = self.metrics_collector.collect_metrics()
                    state["current_metrics"] = {
                        "cpu_percent": current_metrics.cpu_percent,
                        "process_cpu_percent": current_metrics.process_cpu_percent,
                        "memory_percent": current_metrics.memory_percent,
                        "process_memory_mb": current_metrics.process_memory_mb,
                        "gpu_utilization": current_metrics.gpu_utilization,
                        "gpu_memory_percent": current_metrics.gpu_memory_percent
                    }
                except:
                    pass
            
            return state
            
    def print_cluster_state(self):
        """Print a human-readable summary of the cluster state."""
        state = self.get_cluster_state()
        print(f"\n--- Cluster State (View from {self.node_id}) ---")
        print(f"Active Peers: {state['num_active_peers']} ({state['num_ready_peers']} ready)")
        print(f"Requests Scheduled Here: {state['total_requests_scheduled_by_me']}")
        print(f"Local Scheduling Hit Rate: {state['cache_hit_rate_my_decisions']:.2%}")
        print(f"Cluster Queue Depth: {state['cluster_total_queue_depth']}")
        print(f"Cluster Avg Memory: {state['cluster_average_memory']:.2%}")
        
        # Print current metrics if available
        if "current_metrics" in state:
            m = state["current_metrics"]
            print(f"\nCurrent System Metrics:")
            print(f"  CPU: {m['cpu_percent']:.1f}% (process: {m['process_cpu_percent']:.1f}%)")
            print(f"  Memory: {m['memory_percent']:.1f}% (process: {m['process_memory_mb']:.1f} MB)")
            if m['gpu_utilization'] is not None:
                print(f"  GPU: {m['gpu_utilization']:.1f}% util, {m['gpu_memory_percent']:.1f}% mem")
        
        print("\nPeer States (My View):")
        for worker_id, (data, ts, addr) in state['peers'].items():
            status = "✓" if data.get('is_ready', False) else "✗"
            age = time.time() - ts
            addr_str = f"{addr[0]}:{addr[1]}"
            print(f"  {worker_id} @ {addr_str} [{status}] (age: {age:.1f}s): "
                  f"models={data['models_loaded']}, "
                  f"queue={data['queue_depth']}, "
                  f"mem={data['memory_utilization']:.2f}")

def main():
    """Main entry point for running a decentralized node."""
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Decentralized Node with Fault Injection")
    
    # ... (all arguments are the same) ...
    parser.add_argument("--node-id", required=True, help="Unique worker ID")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, required=True, help="Port to bind to")
    parser.add_argument("--seed-nodes", nargs='+',
                        help="List of seed nodes (e.g., 127.0.0.1:9000 127.0.0.1:9001)")
    parser.add_argument("--gossip-interval", type=float, default=1.0,
                        help="Gossip interval in seconds")
    parser.add_argument("--preload-models", nargs='+', default=[],
                        help="List of models to load on startup (e.g., opt-1.3b)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--use-real-models", action="store_true",
                        help="Use real PyTorch models (default: simulated)")
    parser.add_argument("--gcs-bucket", default="remote_model",
                        help="GCS bucket for models")
    parser.add_argument("--cache-dir", default="/tmp/model_cache",
                        help="Local cache directory for models")
    parser.add_argument("--device", default="cpu",
                        help="PyTorch device (cpu, cuda, cuda:0, etc.)")
    parser.add_argument("--enable-metrics", action="store_true", default=True,
                        help="Enable metrics collection")
    parser.add_argument("--disable-gpu-metrics", action="store_true",
                        help="Disable GPU metrics collection")
    parser.add_argument("--metrics-interval", type=float, default=1.0,
                        help="Metrics collection interval (seconds)")
    parser.add_argument("--metrics-log", type=str,
                        help="Log metrics to file (JSON format)")
    parser.add_argument("--metrics-csv", type=str,
                        help="Export metrics to CSV on shutdown")
    
    # Fault injection arguments
    parser.add_argument("--fault-schedule", type=str,
                        help="Fault schedule: 'time:mode:duration,time:mode:duration' "
                             "(e.g., '30:clean_shutdown:10,60:crash:15')")
    parser.add_argument("--fault-config", type=str,
                        help="Path to fault configuration file")
    parser.add_argument("--enable-fault-injection", action="store_true",
                        help="Enable fault injection (required for faults to be injected)")

    args = parser.parse_args()

    # Parse seed nodes
    seed_node_list = []
    if args.seed_nodes:
        for seed in args.seed_nodes:
            try:
                host, port = seed.split(':')
                seed_node_list.append((host, int(port)))
            except ValueError:
                print(f"Invalid seed node format: {seed}. Use host:port")
    
    if not seed_node_list:
        print("Warning: No seed nodes provided. This node may be isolated.")

    # Create node
    node = DecentralizedNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        seed_nodes=seed_node_list,
        gossip_interval=args.gossip_interval,
        verbose=args.verbose,
        use_real_models=args.use_real_models,
        gcs_bucket=args.gcs_bucket,
        cache_dir=args.cache_dir,
        device=args.device,
        enable_metrics=args.enable_metrics,
        enable_gpu_metrics=not args.disable_gpu_metrics,
        metrics_interval=args.metrics_interval,
        metrics_log_file=args.metrics_log
    )

    # Setup fault injection controller
    fault_controller = None
    if args.enable_fault_injection:
        try:
            from fault_injection import (
                FaultInjectionController,
                FailureMode,
                parse_fault_schedule_from_file
            )
            
            fault_controller = FaultInjectionController(node, verbose=True)
            
            # Load faults from schedule string
            if args.fault_schedule:
                fault_controller.add_fault_schedule(args.fault_schedule)
            
            # Load faults from config file
            if args.fault_config:
                faults = parse_fault_schedule_from_file(args.fault_config)
                for fault in faults:
                    fault_controller.add_fault(
                        fault.time_offset,
                        fault.mode,
                        fault.duration
                    )
            
            if fault_controller.faults:
                print(f"\n{'='*70}")
                print("FAULT INJECTION ENABLED")
                print(f"{'='*70}")
                for i, fault in enumerate(fault_controller.faults, 1):
                    print(f"  {i}. {fault}")
                print(f"{'='*70}\n")
            else:
                print("Warning: Fault injection enabled but no faults scheduled")
        except ImportError:
            print("ImportError: fault_injection module not found. Skipping")

    def signal_handler(sig, frame):
        print(f"\n\nShutting down node {args.node_id}...")
        
        # Stop fault injection first
        if fault_controller:
            fault_controller.stop()
            stats = fault_controller.get_stats()
            print(f"\nFault Injection Stats:")
            print(f"  Failures Injected: {stats['failures_injected']}/{stats['faults_scheduled']}")
            print(f"  Recoveries Completed: {stats['recoveries_completed']}")
        
        # Print cluster state
        try:
            node.print_cluster_state()
        except:
            pass
        
        # Export metrics to CSV if requested
        if args.metrics_csv and node.metrics_collector:
            try:
                node.metrics_collector.export_to_csv(args.metrics_csv)
                print(f"Exported metrics to {args.metrics_csv}")
            except Exception as e:
                print(f"Failed to export metrics: {e}")
        
        # Stop node
        try:
            node.stop()
        except:
            pass
        
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Initialize models
    if args.preload_models:
        node.initialize(args.preload_models)
    
    # Start node
    node.start()
    
    # Start fault injection
    if fault_controller and fault_controller.faults:
        fault_controller.start()
    
    print(f"Node {args.node_id} running on {args.host}:{args.port}")
    if fault_controller and fault_controller.faults:
        print(f"Fault injection active with {len(fault_controller.faults)} scheduled events")
    print("Press Ctrl+C to stop")

    # Main loop
    try:
        while True:
            time.sleep(5)
            
            # Only simulate workload if not in a fault state
            if node.is_running and not node.is_paused:
                node.simulate_workload_change()
            
            if args.verbose and node.is_running and not node.is_paused:
                node.print_cluster_state()
                
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == "__main__":
    main()
