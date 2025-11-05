"""
Decentralized node for peer-to-peer scheduling and work.
"""

import socket
import threading
import pickle
import time
import random
import logging
from typing import List, Optional, Dict, Tuple, Any
from multinode.contracts import (
    WorkerLoadReport, ScheduleRequest, ScheduleResponse, PlacementAction
)
from multinode.model_loader import ModelLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(threadName)s] %(message)s')


class DecentralizedNode:
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
        device: str = "cpu"
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr = (host, port)
        self.seed_nodes = seed_nodes
        self.gossip_interval = gossip_interval
        self.worker_timeout = worker_timeout
        self.verbose = verbose
        self.use_real_models = use_real_models

        self.queue_depth: int = 0
        self.memory_utilization: float = 0.0
        self.is_ready: bool = False
        self.initialization_time: float = 0.0

        if use_real_models:
            self.model_loader = ModelLoader(gcs_bucket=gcs_bucket, cache_dir=cache_dir, device=device)
        else:
            self.model_loader = None
            self._simulated_models: List[str] = []

        self.cluster_state: Dict[str, Tuple[WorkerLoadReport, float, Tuple[str, int]]] = {}
        self.state_lock = threading.RLock()

        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.is_running = True
        self._listener_thread = None
        self._gossip_thread = None

        self.logger = logging.getLogger(f"Node-{node_id}")
        if self.verbose:
            self.logger.info(f"Node {node_id} initializing at {host}:{port} (gossip={gossip_interval}s)")

    # --- helpers ---

    def _send_message(self, message: Dict, addr: Tuple[str, int]):
        try:
            serialized = pickle.dumps(message)
            self.sock.sendto(serialized, addr)
        except (socket.error, pickle.PicklingError) as e:
            self.logger.warning(f"Failed to send message to {addr}: {e}")

    def _get_random_peer(self) -> Optional[Tuple[str, int]]:
        with self.state_lock:
            peer_addresses = [
                addr for wid, (_, _, addr) in self.cluster_state.items()
                if wid != self.node_id
            ]
            if peer_addresses:
                return random.choice(peer_addresses)
            elif self.seed_nodes:
                return random.choice(self.seed_nodes)
            else:
                return None

    def _create_own_report(self) -> WorkerLoadReport:
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

    # --- threads ---

    def _listener(self):
        if self.verbose:
            self.logger.info(f"{self.node_id} listener started")
        while self.is_running:
            try:
                raw_data, addr = self.sock.recvfrom(8192)
                message = pickle.loads(raw_data)
                msg_type = message.get('type')

                if msg_type == 'schedule_request':
                    request = ScheduleRequest.from_dict(message['payload'])
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

            except (socket.error, pickle.UnpicklingError, EOFError) as e:
                if self.is_running:
                    self.logger.error(f"{self.node_id} listener error: {e}", exc_info=True)
                break
        if self.verbose:
            self.logger.info(f"{self.node_id} listener stopped")

    def _gossip(self):
        if self.verbose:
            self.logger.info(f"{self.node_id} gossip thread started")
        while self.is_running:
            time.sleep(self.gossip_interval)
            try:
                with self.state_lock:
                    report = self._create_own_report()
                    self.cluster_state[self.node_id] = (report, time.time(), self.addr)
                    self._cleanup_dead_workers()
                    target_addr = self._get_random_peer()
                    if not target_addr or target_addr == self.addr:
                        continue
                    payload = {wid: (r.to_dict(), ts, a) for wid, (r, ts, a) in self.cluster_state.items()}
                    message = {'type': 'gossip_sync', 'payload': payload}
                self._send_message(message, target_addr)
            except Exception as e:
                self.logger.error(f"Error in gossip thread: {e}", exc_info=True)
        if self.verbose:
            self.logger.info(f"{self.node_id} gossip thread stopped")

    def _merge_cluster_state(self, incoming_state: Dict[str, Tuple[Dict, float, Tuple[str, int]]]):
        with self.state_lock:
            for worker_id, (report_dict, timestamp, addr) in incoming_state.items():
                if worker_id == self.node_id:
                    continue
                if (worker_id not in self.cluster_state) or (timestamp > self.cluster_state[worker_id][1]):
                    try:
                        report = WorkerLoadReport.from_dict(report_dict)
                        self.cluster_state[worker_id] = (report, timestamp, addr)
                    except Exception as e:
                        self.logger.warning(f"Failed to deserialize report for {worker_id}: {e}")

    def _cleanup_dead_workers(self):
        current_time = time.time()
        dead_workers = [
            worker_id
            for worker_id, (_, last_seen, _) in self.cluster_state.items()
            if current_time - last_seen > self.worker_timeout and worker_id != self.node_id
        ]
        for worker_id in dead_workers:
            del self.cluster_state[worker_id]
            self.logger.warning(f"Removed dead worker: {worker_id}")

    def _get_active_workers(self) -> Dict[str, WorkerLoadReport]:
        current_time = time.time()
        return {
            worker_id: report
            for worker_id, (report, ts, _) in self.cluster_state.items()
            if (current_time - ts) <= self.worker_timeout or worker_id == self.node_id
        }

    # --- scheduling ---

    def _calculate_worker_score(self, report: WorkerLoadReport, has_model: bool) -> float:
        if not report.is_ready:
            return -float('inf')
        score = 1000.0 if has_model else 0.0
        score -= report.queue_depth * 10
        score -= report.memory_utilization * 100
        return score

    def schedule(self, request: ScheduleRequest) -> ScheduleResponse:
        self.total_requests += 1
        with self.state_lock:
            active_workers = self._get_active_workers()
            if not active_workers:
                raise RuntimeError("No active workers available")

            workers_with_model = []
            workers_without_model = []

            for worker_id, report in active_workers.items():
                has_model = request.model_required in report.models_loaded
                score = self._calculate_worker_score(report, has_model)
                (workers_with_model if has_model else workers_without_model).append((worker_id, report, score))

            if workers_with_model:
                self.cache_hits += 1
                workers_with_model.sort(key=lambda x: x[2], reverse=True)
                best_worker_id, best_report, _ = workers_with_model[0]
                action = PlacementAction.SERVE
                reason = f"Worker {best_worker_id} has model. Queue: {best_report.queue_depth}"
                estimated_wait = best_report.queue_depth * 0.5
            elif workers_without_model:
                self.cache_misses += 1
                workers_without_model.sort(key=lambda x: x[2], reverse=True)
                best_worker_id, best_report, _ = workers_without_model[0]
                action = PlacementAction.COLD_START
                reason = f"No worker has model. Cold starting on {best_worker_id}."
                estimated_wait = 10.0 + (best_report.queue_depth * 0.5)
            else:
                raise RuntimeError("No ready workers available")

            if self.verbose:
                self.logger.info(
                    f"Scheduled {request.request_id} ({request.model_required}) to {best_worker_id} ({action.value})"
                )

            return ScheduleResponse(
                worker_id=best_worker_id,
                action=action,
                estimated_wait_time=estimated_wait,
                reason=reason
            )

    # --- worker ops ---

    def get_loaded_models(self) -> List[str]:
        if self.use_real_models:
            return self.model_loader.get_loaded_models()
        return self._simulated_models.copy()

    def load_model(self, model_id: str) -> bool:
        with self.state_lock:
            if self.use_real_models:
                return self.model_loader.load_model(model_id)
            else:
                if model_id not in self._simulated_models:
                    self._simulated_models.append(model_id)
                return True

    def unload_model(self, model_id: str) -> bool:
        with self.state_lock:
            if self.use_real_models:
                return self.model_loader.unload_model(model_id)
            else:
                if model_id in self._simulated_models:
                    self._simulated_models.remove(model_id)
                return True

    def _get_memory_utilization(self) -> float:
        try:
            import subprocess, os, re
            pid = os.getpid()
            try:
                result = subprocess.run(
                    ['top', '-pid', str(pid), '-l', '1', '-stats', 'mem'],
                    capture_output=True, text=True, timeout=1
                )
                lines = result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    mem_str = lines[-1].strip()
                    match = re.search(r'([\d.]+)([MG])', mem_str)
                    if match:
                        value = float(match.group(1))
                        unit = match.group(2)
                        mem_gb = value / 1024 if unit == 'M' else value
                        total_mem = self._get_total_memory()
                        return min(1.0, mem_gb / total_mem) if total_mem > 0 else 0.0
            except Exception:
                result = subprocess.run(
                    ['ps', '-p', str(pid), '-o', '%mem'],
                    capture_output=True, text=True, timeout=1
                )
                lines = result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    mem_percent = float(lines[1].strip())
                    return mem_percent / 100.0
        except Exception:
            pass
        return self.memory_utilization

    def _get_total_memory(self) -> float:
        try:
            import subprocess
            try:
                result = subprocess.run(['sysctl', '-n', 'hw.memsize'], capture_output=True, text=True, timeout=1)
                bytes_mem = int(result.stdout.strip())
                return bytes_mem / (1024**3)
            except Exception:
                with open('/proc/meminfo', 'r') as f:
                    for line in f:
                        if line.startswith('MemTotal:'):
                            kb = int(line.split()[1])
                            return kb / (1024**2)
        except Exception:
            pass
        return 16.0

    def set_queue_depth(self, depth: int):
        with self.state_lock:
            self.queue_depth = depth

    def simulate_workload_change(self):
        with self.state_lock:
            self.queue_depth = max(0, self.queue_depth + random.randint(-2, 3))
            self.memory_utilization = max(0.0, min(1.0, self.memory_utilization + random.uniform(-0.1, 0.1)))

    def initialize(self, models_to_load: List[str]) -> float:
        start_time = time.time()
        self.logger.info(f"{self.node_id} initializing with models: {models_to_load}")
        for model_id in models_to_load:
            self.logger.info(f"{self.node_id} loading model {model_id}...")
            self.load_model(model_id)
        self.initialization_time = time.time() - start_time
        self.is_ready = True
        self.logger.info(f"{self.node_id} initialization complete in {self.initialization_time:.2f}s.")
        return self.initialization_time

    # lifecycle

    def start(self):
        if not self.is_ready and self.use_real_models:
            self.logger.warning(f"{self.node_id} starting without initialization. Call initialize() first.")
            self.is_ready = True
        if not self.use_real_models:
            self.is_ready = True

        self.sock.bind(self.addr)
        self.logger.info(f"Node {self.node_id} bound to {self.addr}")

        self._listener_thread = threading.Thread(target=self._listener, name=f"{self.node_id}-Listener")
        self._gossip_thread = threading.Thread(target=self._gossip, name=f"{self.node_id}-Gossip")
        self._listener_thread.start()
        self._gossip_thread.start()
        self.logger.info(f"{self.node_id} started (ready: {self.is_ready})")

    def stop(self):
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

    # visibility

    def get_cluster_state(self) -> Dict:
        with self.state_lock:
            active_workers = self._get_active_workers()
            total_queue = sum(w.queue_depth for w in active_workers.values())
            avg_memory = (sum(w.memory_utilization for w in active_workers.values()) / len(active_workers)
                          if active_workers else 0.0)
            ready_workers = sum(1 for w in active_workers.values() if w.is_ready)
            peer_data = {}
            for worker_id in active_workers.keys():
                if worker_id in self.cluster_state:
                    report, ts, addr = self.cluster_state[worker_id]
                    peer_data[worker_id] = (report.to_dict(), ts, addr)
            return {
                "node_id": self.node_id,
                "num_active_peers": len(active_workers),
                "num_ready_peers": ready_workers,
                "total_requests_scheduled_by_me": self.total_requests,
                "cache_hit_rate_my_decisions": (self.cache_hits / self.total_requests) if self.total_requests else 0.0,
                "cluster_total_queue_depth": total_queue,
                "cluster_average_memory": avg_memory,
                "peers": peer_data
            }

    def print_cluster_state(self):
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
                  f"models={data['models_loaded']}, queue={data['queue_depth']}, mem={data['memory_utilization']:.2f}")


def main():
    import argparse
    import signal
    parser = argparse.ArgumentParser(description="Decentralized Node")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--seed-nodes", nargs='+')
    parser.add_argument("--gossip-interval", type=float, default=1.0)
    parser.add_argument("--preload-models", nargs='+', default=[])
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    seed_node_list = []
    if args.seed_nodes:
        for seed in args.seed_nodes:
            try:
                h, p = seed.split(':')
                seed_node_list.append((h, int(p)))
            except ValueError:
                print(f"Invalid seed node format: {seed}. Use host:port")

    node = DecentralizedNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        seed_nodes=seed_node_list,
        gossip_interval=args.gossip_interval,
        verbose=args.verbose,
        use_real_models=False
    )

    def signal_handler(sig, frame):
        print(f"\nShutting down node {args.node_id}...")
        node.stop()
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    if args.preload_models:
        node.initialize(args.preload_models)
    node.start()

    print(f"Node {args.node_id} running on {args.host}:{args.port}")
    print("Press Ctrl+C to stop")
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
