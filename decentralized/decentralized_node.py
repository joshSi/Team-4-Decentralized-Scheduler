"""
Decentralized node for peer-to-peer scheduling and work.

This node merges the responsibilities of a worker and a coordinator.
- It performs work (loads models, has a queue) like a WorkerNode.
- It maintains a view of the entire cluster state, like a CentralCoordinator.
- It uses a gossip protocol to synchronize its cluster state with other peers.
- It can receive a schedule request from a client and make a placement
  decision based on its local, eventually-consistent view of the cluster.
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
        device: str = "cpu" # We probably won't be able to use GPUs due to availability
    ):
        """
        Initialize a decentralized node.

        Args:
            node_id: Unique identifier for this worker
            host: Host address for this worker
            port: Port for this worker to listen on
            seed_nodes: List of (host, port) tuples for bootstrapping
            gossip_interval: Seconds between gossip syncs
            worker_timeout: Time (seconds) after which a worker is considered dead
            verbose: Enable detailed logging
            use_real_models: If True, use actual PyTorch model loading from GCS
            gcs_bucket: GCS bucket name for model storage
            cache_dir: Local cache directory for models
            device: PyTorch device ('cpu', 'cuda', etc.)
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

        # --- Networking & Threading ---
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.is_running = True
        self._listener_thread = None
        self._gossip_thread = None

        self.logger = logging.getLogger(f"Node-{node_id}")
        if self.verbose:
            self.logger.info(
                f"Node {node_id} initializing at {host}:{port}. "
                f"Gossip interval: {gossip_interval}s"
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
        Get a random peer from the cluster state (excluding self).
        Falls back to a seed node if no other peers are known.
        """
        with self.state_lock:
            # Get addresses from all known peers, excluding ourselves
            peer_addresses = [
                addr for wid, (_, _, addr) in self.cluster_state.items()
                if wid != self.node_id
            ]

            if peer_addresses:
                # We know other peers, pick one at random
                return random.choice(peer_addresses)
            elif self.seed_nodes:
                # We don't know any peers yet, pick a seed to bootstrap
                return random.choice(self.seed_nodes)
            else:
                # Isolated node
                return None
            
    def _create_own_report(self) -> WorkerLoadReport:
        """Helper to generate a load report for this node."""
        with self.state_lock:
            if self.use_real_models and self.is_ready:
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
            time.sleep(self.gossip_interval)
            
            try:
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
        Get actual memory utilization of current process using top command.

        Returns:
            Memory utilization as fraction (0.0 to 1.0)
        """
        try:
            import subprocess
            import os

            pid = os.getpid()
            # Try macOS format first
            try:
                result = subprocess.run(
                    ['top', '-pid', str(pid), '-l', '1', '-stats', 'mem'],
                    capture_output=True,
                    text=True,
                    timeout=1
                )
                lines = result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    mem_str = lines[-1].strip()
                    import re
                    match = re.search(r'([\d.]+)([MG])', mem_str)
                    if match:
                        value = float(match.group(1))
                        unit = match.group(2)
                        mem_gb = value / 1024 if unit == 'M' else value
                        total_mem = self._get_total_memory()
                        return min(1.0, mem_gb / total_mem) if total_mem > 0 else 0.0
            except (subprocess.SubprocessError, FileNotFoundError):
                # Try Linux format
                result = subprocess.run(
                    ['ps', '-p', str(pid), '-o', '%mem'],
                    capture_output=True,
                    text=True,
                    timeout=1
                )
                lines = result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    mem_percent = float(lines[1].strip())
                    return mem_percent / 100.0
        except Exception as e:
            self.logger.warning(f"Failed to get memory utilization: {e}")

        # Fallback to current value if command fails
        return self.memory_utilization

    def _get_total_memory(self) -> float:
        """
        Get total system memory in GB.

        Returns:
            Total memory in GB
        """
        try:
            import subprocess
            # Try macOS
            try:
                result = subprocess.run(
                    ['sysctl', '-n', 'hw.memsize'],
                    capture_output=True,
                    text=True,
                    timeout=1
                )
                bytes_mem = int(result.stdout.strip())
                return bytes_mem / (1024**3)  # Convert to GB
            except (subprocess.SubprocessError, FileNotFoundError):
                # Try Linux
                with open('/proc/meminfo', 'r') as f:
                    for line in f:
                        if line.startswith('MemTotal:'):
                            kb = int(line.split()[1])
                            return kb / (1024**2)  # Convert to GB
        except Exception:
            pass
        return 16.0  # Default fallback to 16GB

    def set_queue_depth(self, depth: int):
        with self.state_lock:
            self.queue_depth = depth

    def simulate_workload_change(self):
        """Simulate random workload changes for testing."""
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

            return {
                "node_id": self.node_id,
                "num_active_peers": len(active_workers),
                "num_ready_peers": ready_workers,
                "total_requests_scheduled_by_me": self.total_requests,
                "cache_hit_rate_my_decisions": self.cache_hits / self.total_requests if self.total_requests > 0 else 0.0,
                "cluster_total_queue_depth": total_queue,
                "cluster_average_memory": avg_memory,
                "peers": peer_data
            }
            
    def print_cluster_state(self):
        """Print a human-readable summary of the cluster state."""
        state = self.get_cluster_state()
        print(f"\n--- Cluster State (View from {self.node_id}) ---")
        print(f"Active Peers: {state['num_active_peers']} ({state['num_ready_peers']} ready)")
        print(f"Requests Scheduled Here: {state['total_requests_scheduled_by_me']}")
        print(f"Local Scheduling Hit Rate: {state['cache_hit_rate_my_decisions']:.2%}")
        print(f"Cluster Queue Depth: {state['cluster_total_queue_depth']}")
        print(f"Cluster Avg Memory: {state['cluster_average_memory']:.2%}")
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

    parser = argparse.ArgumentParser(description="Decentralized Node")
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
    # Add other args from worker_node if needed (use_real_models, gcs_bucket, etc.)

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


    node = DecentralizedNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        seed_nodes=seed_node_list,
        gossip_interval=args.gossip_interval,
        verbose=args.verbose,
        use_real_models=False # Default to simulation
    )

    def signal_handler(sig, frame):
        print(f"\n\nShutting down node {args.node_id}...")
        node.stop()
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Initialize and start
    if args.preload_models:
        node.initialize(args.preload_models)
    
    node.start()
    
    print(f"Node {args.node_id} running on {args.host}:{args.port}")
    print("Press Ctrl+C to stop")

    # Simulate workload and print stats
    try:
        while True:
            time.sleep(5)
            node.simulate_workload_change()
            if args.verbose:
                node.print_cluster_state()
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == "__main__":
    main()
