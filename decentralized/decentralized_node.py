"""
Decentralized gossip-based node for peer-to-peer scheduling.

This node acts as both a worker and a scheduler:
- Maintains own worker state (loaded models, queue depth, etc.)
- Gossips state with peer nodes, maintains eventual consistency
- Accepts schedule requests from clients and makes placement decisions
- Uses UDP and pickle serialization matching the existing architecture
"""

import socket
import threading
import pickle
import time
import random
import logging
from typing import Dict, Tuple, Optional, List
from contracts import (
    WorkerLoadReport,
    ScheduleRequest,
    ScheduleResponse,
    PlacementAction
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(threadName)s] %(message)s')


class GossipNode:
    """
    Decentralized node that gossips state and handles scheduling.
    
    Each node:
    1. Maintains its own worker state
    2. Periodically gossips state to random peers
    3. Merges incoming gossip to maintain cluster view
    4. Accepts and responds to client schedule requests
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 9000,
        seed_nodes: List[Tuple[str, int]] = None,
        gossip_interval: float = 1.0,
        worker_timeout: float = 10.0,
        verbose: bool = True
    ):
        """
        Initialize the gossip node.

        Args:
            node_id: Unique identifier for this node
            host: Host address to bind to
            port: UDP port to listen on
            seed_nodes: List of (host, port) for initial peers
            gossip_interval: Seconds between gossip rounds
            worker_timeout: Time after which a peer is considered dead
            verbose: Enable detailed logging
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr = (host, port)
        self.seed_nodes = seed_nodes or []
        self.gossip_interval = gossip_interval
        self.worker_timeout = worker_timeout
        self.verbose = verbose

        # Own worker state
        self.loaded_models: List[str] = []
        self.queue_depth: int = 0
        self.memory_utilization: float = 0.0
        self.is_ready: bool = False

        # Cluster state: {node_id: (WorkerLoadReport, timestamp, address)}
        self.cluster_state: Dict[str, Tuple[WorkerLoadReport, float, Tuple[str, int]]] = {}
        self.state_lock = threading.RLock()

        # Scheduling statistics
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0

        # Networking
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.is_running = True
        self._listener_thread = None
        self._gossip_thread = None

        self.logger = logging.getLogger(f"GossipNode-{node_id}")
        if self.verbose:
            self.logger.info(
                f"Gossip node {node_id} initialized at {host}:{port}. "
                f"Gossip interval: {gossip_interval}s"
            )

    def _create_own_report(self) -> WorkerLoadReport:
        """Generate a load report for this node."""
        return WorkerLoadReport(
            node_id=self.node_id,
            models_loaded=self.loaded_models.copy(),
            queue_depth=self.queue_depth,
            memory_utilization=self.memory_utilization,
            is_ready=self.is_ready,
            timestamp=time.time()
        )

    def _send_message(self, message: Dict, addr: Tuple[str, int]):
        """Serialize and send a UDP message."""
        try:
            serialized = pickle.dumps(message)
            self.sock.sendto(serialized, addr)
        except (socket.error, pickle.PicklingError) as e:
            self.logger.warning(f"Failed to send message to {addr}: {e}")

    def _get_random_peer(self) -> Optional[Tuple[str, int]]:
        """
        Get a random peer address from cluster state or seed nodes.
        
        Returns:
            Random peer address, or None if no peers available
        """
        with self.state_lock:
            # Get all known peer addresses except ourselves
            peer_addresses = [
                addr for nid, (_, _, addr) in self.cluster_state.items()
                if nid != self.node_id and addr != self.addr
            ]

            if peer_addresses:
                return random.choice(peer_addresses)
            elif self.seed_nodes:
                # Bootstrap from seed nodes
                return random.choice(self.seed_nodes)
            else:
                return None

    def _listener(self):
        """Listen for incoming UDP messages."""
        if self.verbose:
            self.logger.info(f"{self.node_id} listener started")

        while self.is_running:
            try:
                raw_data, sender_addr = self.sock.recvfrom(8192)
                message = pickle.loads(raw_data)
                msg_type = message.get('type')

                if msg_type == 'gossip_sync':
                    # Merge incoming cluster state
                    self._handle_gossip_sync(message['payload'])

                elif msg_type == 'schedule_request':
                    # Handle client request for scheduling
                    self._handle_schedule_request(message['payload'], sender_addr)

                else:
                    self.logger.debug(f"Unknown message type: {msg_type}")

            except (socket.error, pickle.UnpicklingError, EOFError) as e:
                if self.is_running:
                    self.logger.error(f"{self.node_id} listener error: {e}", exc_info=True)
                break

        if self.verbose:
            self.logger.info(f"{self.node_id} listener stopped")

    def _gossip(self):
        """Periodically gossip cluster state to random peers."""
        if self.verbose:
            self.logger.info(f"{self.node_id} gossip thread started")

        while self.is_running:
            time.sleep(self.gossip_interval)

            try:
                with self.state_lock:
                    # Update own state in cluster
                    report = self._create_own_report()
                    self.cluster_state[self.node_id] = (report, time.time(), self.addr)

                    # Cleanup dead peers
                    self._cleanup_dead_peers()

                    # Select random peer to gossip to
                    target_addr = self._get_random_peer()
                    if not target_addr:
                        self.logger.debug("No peers to gossip to")
                        continue

                    # Prepare gossip payload
                    payload = {
                        nid: (report.to_dict(), ts, addr)
                        for nid, (report, ts, addr) in self.cluster_state.items()
                    }

                message = {'type': 'gossip_sync', 'payload': payload}
                self._send_message(message, target_addr)

                if self.verbose:
                    self.logger.debug(
                        f"Gossiped state ({len(payload)} nodes) to {target_addr}"
                    )

            except Exception as e:
                self.logger.error(f"Error in gossip thread: {e}", exc_info=True)

        if self.verbose:
            self.logger.info(f"{self.node_id} gossip thread stopped")

    def _handle_gossip_sync(self, incoming_state: Dict):
        """
        Merge incoming gossip state with local state.
        
        Args:
            incoming_state: Dict of {node_id: (report_dict, timestamp, address)}
        """
        with self.state_lock:
            merged_count = 0
            new_peers = []

            for node_id, (report_dict, timestamp, addr) in incoming_state.items():
                if node_id == self.node_id:
                    continue  # Skip our own state

                # Update if newer or new peer
                if (
                    node_id not in self.cluster_state or
                    timestamp > self.cluster_state[node_id][1]
                ):
                    if node_id not in self.cluster_state:
                        new_peers.append(node_id)
                        print(
                            f"METRIC_PEER_DISCOVERY:{self.node_id},{node_id},{time.time()}",
                            flush=True
                        )

                    try:
                        report = WorkerLoadReport.from_dict(report_dict)
                        self.cluster_state[node_id] = (report, timestamp, addr)
                        merged_count += 1
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to deserialize report for {node_id}: {e}"
                        )

            if self.verbose and new_peers:
                self.logger.info(f"Discovered {len(new_peers)} new peers: {new_peers}")
            elif self.verbose and merged_count > 0:
                self.logger.debug(f"Merged state for {merged_count} nodes")

    def _cleanup_dead_peers(self):
        """Remove peers that haven't been seen recently."""
        current_time = time.time()
        dead_peers = [
            nid for nid, (_, last_seen, _) in self.cluster_state.items()
            if current_time - last_seen > self.worker_timeout and nid != self.node_id
        ]

        for nid in dead_peers:
            del self.cluster_state[nid]
            self.logger.warning(f"Removed dead peer: {nid}")
            print(
                f"METRIC_FAILURE_DETECTED:{self.node_id},{nid},{time.time()}",
                flush=True
            )

    def _get_active_peers(self) -> Dict[str, WorkerLoadReport]:
        """Get all active peers including self."""
        current_time = time.time()
        return {
            nid: report
            for nid, (report, ts, _) in self.cluster_state.items()
            if (current_time - ts) <= self.worker_timeout or nid == self.node_id
        }

    def _calculate_worker_score(
        self,
        report: WorkerLoadReport,
        has_model: bool
    ) -> float:
        """
        Calculate placement score for a worker (higher is better).
        
        Args:
            report: Worker's load report
            has_model: Whether worker has required model loaded
            
        Returns:
            Score value (higher is better)
        """
        if not report.is_ready:
            return -float('inf')  # Never schedule to unready nodes

        # Heavily favor nodes with model already loaded
        score = 1000.0 if has_model else 0.0

        # Penalize by queue depth and memory usage
        score -= report.queue_depth * 10
        score -= report.memory_utilization * 100

        return score

    def _handle_schedule_request(
        self,
        payload: Dict,
        sender_addr: Tuple[str, int]
    ):
        """
        Handle schedule request from client and send response.
        
        Args:
            payload: Schedule request payload
            sender_addr: Address to send response to
        """
        try:
            request = ScheduleRequest.from_dict(payload)
            response = self.schedule(request)

            response_message = {
                'type': 'schedule_response',
                'payload': response.to_dict()
            }
            self._send_message(response_message, sender_addr)

            if self.verbose:
                self.logger.info(
                    f"Scheduled {request.request_id} ({request.model_required}) "
                    f"to {response.worker_id} ({response.action.value})"
                )

        except RuntimeError as e:
            # No workers available
            error_message = {
                'type': 'schedule_error',
                'payload': {'error': str(e)}
            }
            self._send_message(error_message, sender_addr)
            self.logger.error(f"Schedule error: {e}")

        except Exception as e:
            self.logger.error(f"Error handling schedule request: {e}", exc_info=True)

    def schedule(self, request: ScheduleRequest) -> ScheduleResponse:
        """
        Find optimal worker based on local cluster view.
        
        Args:
            request: Scheduling request
            
        Returns:
            Schedule response with worker assignment
        """
        self.total_requests += 1

        with self.state_lock:
            active_peers = self._get_active_peers()

            if not active_peers:
                self.logger.error(
                    f"No active peers for request {request.request_id}"
                )
                raise RuntimeError("No active peers available")

            # Partition by model availability
            workers_with_model = []
            workers_without_model = []

            for node_id, report in active_peers.items():
                has_model = request.model_required in report.models_loaded
                score = self._calculate_worker_score(report, has_model)

                if has_model:
                    workers_with_model.append((node_id, report, score))
                else:
                    workers_without_model.append((node_id, report, score))

            # Make placement decision
            if workers_with_model:
                # Cache hit: serve from node with model
                self.cache_hits += 1
                workers_with_model.sort(key=lambda x: x[2], reverse=True)
                best_node_id, best_report, _ = workers_with_model[0]

                estimated_wait = best_report.queue_depth * 0.5
                reason = (
                    f"Worker {best_node_id} has model. "
                    f"Queue: {best_report.queue_depth}"
                )

                return ScheduleResponse(
                    worker_id=best_node_id,
                    action=PlacementAction.SERVE,
                    estimated_wait_time=estimated_wait,
                    reason=reason
                )

            elif workers_without_model:
                # Cache miss: cold start on least loaded
                self.cache_misses += 1
                workers_without_model.sort(key=lambda x: x[2], reverse=True)
                best_node_id, best_report, _ = workers_without_model[0]

                estimated_wait = 10.0 + (best_report.queue_depth * 0.5)
                reason = (
                    f"No worker has model. Cold starting on {best_node_id}."
                )

                return ScheduleResponse(
                    worker_id=best_node_id,
                    action=PlacementAction.COLD_START,
                    estimated_wait_time=estimated_wait,
                    reason=reason
                )

            else:
                # No ready workers
                self.logger.error("No ready peers available")
                raise RuntimeError("No ready peers available")

    def load_model(self, model_id: str):
        """
        Load a model (simulated).
        
        Args:
            model_id: Model identifier
        """
        with self.state_lock:
            if model_id not in self.loaded_models:
                self.loaded_models.append(model_id)
                self.logger.info(f"Loaded model: {model_id}")

    def unload_model(self, model_id: str):
        """
        Unload a model (simulated).
        
        Args:
            model_id: Model identifier
        """
        with self.state_lock:
            if model_id in self.loaded_models:
                self.loaded_models.remove(model_id)
                self.logger.info(f"Unloaded model: {model_id}")

    def set_queue_depth(self, depth: int):
        """Update queue depth."""
        with self.state_lock:
            self.queue_depth = depth

    def simulate_workload_change(self):
        """Simulate random workload changes."""
        with self.state_lock:
            self.queue_depth = max(0, self.queue_depth + random.randint(-2, 3))
            self.memory_utilization = max(
                0.0,
                min(1.0, self.memory_utilization + random.uniform(-0.1, 0.1))
            )

    def initialize(self, models_to_load: List[str]) -> float:
        """
        Initialize node by loading models.
        
        Args:
            models_to_load: List of model IDs to pre-load
            
        Returns:
            Initialization time in seconds
        """
        start_time = time.time()
        self.logger.info(f"{self.node_id} initializing with models: {models_to_load}")

        for model_id in models_to_load:
            self.load_model(model_id)

        initialization_time = time.time() - start_time
        self.is_ready = True

        self.logger.info(
            f"{self.node_id} initialization complete in {initialization_time:.2f}s"
        )
        return initialization_time

    def start(self):
        """Start the node's listener and gossip threads."""
        if not self.is_ready:
            self.logger.warning(
                f"{self.node_id} starting without initialization. "
                "Call initialize() first to pre-load models."
            )
            self.is_ready = True

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
        self.sock.close()

        if self._gossip_thread:
            self._gossip_thread.join()
        if self._listener_thread:
            self._listener_thread.join()

        if self.verbose:
            self.logger.info(f"{self.node_id} stopped")

    def get_cluster_state(self) -> Dict:
        """Get summary of local cluster view."""
        with self.state_lock:
            active_peers = self._get_active_peers()
            total_queue = sum(w.queue_depth for w in active_peers.values())
            avg_memory = (
                sum(w.memory_utilization for w in active_peers.values()) / len(active_peers)
                if active_peers else 0.0
            )
            ready_peers = sum(1 for w in active_peers.values() if w.is_ready)

            peer_data = {}
            for node_id in active_peers.keys():
                if node_id in self.cluster_state:
                    report, ts, addr = self.cluster_state[node_id]
                    peer_data[node_id] = (report.to_dict(), ts, addr)

            return {
                "node_id": self.node_id,
                "num_active_peers": len(active_peers),
                "num_ready_peers": ready_peers,
                "total_requests_scheduled": self.total_requests,
                "cache_hit_rate": (
                    self.cache_hits / self.total_requests
                    if self.total_requests > 0 else 0.0
                ),
                "cluster_total_queue": total_queue,
                "cluster_avg_memory": avg_memory,
                "peers": peer_data
            }

    def print_cluster_state(self):
        """Print human-readable cluster state."""
        state = self.get_cluster_state()
        print(f"\n--- Cluster State (View from {self.node_id}) ---")
        print(f"Active Peers: {state['num_active_peers']} ({state['num_ready_peers']} ready)")
        print(f"Requests Scheduled: {state['total_requests_scheduled']}")
        print(f"Cache Hit Rate: {state['cache_hit_rate']:.2%}")
        print(f"Cluster Queue Depth: {state['cluster_total_queue']}")
        print(f"Cluster Avg Memory: {state['cluster_avg_memory']:.2%}")
        print("\nPeer States:")
        for node_id, (data, ts, addr) in state['peers'].items():
            status = "✓" if data.get('is_ready', False) else "✗"
            age = time.time() - ts
            print(
                f"  {node_id} @ {addr[0]}:{addr[1]} [{status}] (age: {age:.1f}s): "
                f"models={data['models_loaded']}, "
                f"queue={data['queue_depth']}, "
                f"mem={data['memory_utilization']:.2f}"
            )


def main():
    """Main entry point for running a gossip node."""
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Decentralized Gossip Node")
    parser.add_argument("--node-id", required=True, help="Unique node ID")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, required=True, help="Port to bind to")
    parser.add_argument(
        "--seed-nodes",
        nargs='+',
        help="Seed nodes (e.g., 127.0.0.1:9000 127.0.0.1:9001)"
    )
    parser.add_argument(
        "--gossip-interval",
        type=float,
        default=1.0,
        help="Gossip interval in seconds"
    )
    parser.add_argument(
        "--preload-models",
        nargs='+',
        default=[],
        help="Models to load on startup"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")

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

    node = GossipNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        seed_nodes=seed_node_list,
        gossip_interval=args.gossip_interval,
        verbose=args.verbose
    )

    def signal_handler(sig, frame):
        print(f"\n\nShutting down node {args.node_id}...")
        node.print_cluster_state()
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
