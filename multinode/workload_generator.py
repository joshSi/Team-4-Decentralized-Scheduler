"""
Workload trace generator for LLM inference requests (Gamma CV=8).
"""

import numpy as np
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


@dataclass
class RequestTrace:
    request_id: str
    timestamp: float
    model_id: str
    dataset_source: str
    data: Dict[str, Any]


class WorkloadGenerator:
    def __init__(self, target_rps: float = 10.0, cv: float = 8.0, duration: float = 300.0,
                 models: Optional[List[str]] = None, seed: Optional[int] = 42):
        self.target_rps = target_rps
        self.cv = cv
        self.duration = duration
        self.models = models or ['opt-1.3b', 'opt-2.7b']
        self.logger = logging.getLogger("WorkloadGenerator")
        if seed is not None:
            np.random.seed(seed)
        self.shape = 1.0 / (cv ** 2)
        self.mean_inter_arrival = 1.0 / target_rps
        self.scale = self.mean_inter_arrival / self.shape
        self.logger.info(
            f"Workload generator: RPS={target_rps}, CV={cv}, shape={self.shape:.4f}, scale={self.scale:.4f}"
        )
        self.trace: List[RequestTrace] = []

    def generate_inter_arrival_times(self, num_requests: Optional[int] = None) -> np.ndarray:
        if num_requests is None:
            num_requests = int(self.duration * self.target_rps * 1.5)
        arr = np.random.gamma(shape=self.shape, scale=self.scale, size=num_requests)
        return arr

    def generate_trace(self, dataset_samples: Optional[List[Dict[str, Any]]] = None) -> List[RequestTrace]:
        inter = self.generate_inter_arrival_times()
        arrival_times = np.cumsum(inter)
        mask = arrival_times <= self.duration
        arrival_times = arrival_times[mask]
        num_requests = len(arrival_times)
        self.logger.info(f"Trace: {num_requests} requests over {self.duration}s (actual {num_requests/self.duration:.2f} rps)")

        self.trace = []
        for i, ts in enumerate(arrival_times):
            model_id = self.models[i % len(self.models)]
            if dataset_samples and i < len(dataset_samples):
                sample = dataset_samples[i]
                dataset_source = sample.get('dataset', 'unknown')
                data = sample.get('data', {})
            else:
                dataset_source = 'synthetic'
                data = {'request_index': i}
            self.trace.append(RequestTrace(
                request_id=f"req-{i}", timestamp=ts, model_id=model_id,
                dataset_source=dataset_source, data=data
            ))
        return self.trace

    def get_trace(self) -> List[RequestTrace]:
        if not self.trace:
            self.generate_trace()
        return self.trace

    def get_trace_stats(self) -> Dict[str, Any]:
        if not self.trace:
            return {}
        timestamps = [r.timestamp for r in self.trace]
        inter = np.diff([0] + timestamps)
        return {
            'num_requests': len(self.trace),
            'duration': self.duration,
            'actual_rps': len(self.trace) / self.duration,
            'target_rps': self.target_rps,
            'mean_inter_arrival': float(np.mean(inter)),
            'std_inter_arrival': float(np.std(inter)),
            'cv_inter_arrival': float(np.std(inter) / np.mean(inter)),
            'min_inter_arrival': float(np.min(inter)),
            'max_inter_arrival': float(np.max(inter)),
            'models': list(set(r.model_id for r in self.trace)),
            'datasets': list(set(r.dataset_source for r in self.trace)),
        }
