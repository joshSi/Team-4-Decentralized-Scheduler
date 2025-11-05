"""
Data contracts for the decentralized cluster.
"""

from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from enum import Enum
import json


class PlacementAction(Enum):
    SERVE = "serve"
    COLD_START = "cold_start"
    MIGRATE = "migrate"


@dataclass
class WorkerLoadReport:
    node_id: str
    models_loaded: List[str]
    queue_depth: int
    memory_utilization: float
    is_ready: bool = False
    timestamp: Optional[float] = None

    def to_dict(self) -> Dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str):
        return cls.from_dict(json.loads(json_str))

    def is_overloaded(self, queue_threshold: int = 10, memory_threshold: float = 0.9) -> bool:
        return self.queue_depth > queue_threshold or self.memory_utilization > memory_threshold


@dataclass
class ScheduleRequest:
    request_id: str
    model_required: str
    priority: Optional[int] = 0

    def to_dict(self) -> Dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict) -> 'ScheduleRequest':
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> 'ScheduleRequest':
        return cls.from_dict(json.loads(json_str))


@dataclass
class ScheduleResponse:
    worker_id: str
    action: PlacementAction
    estimated_wait_time: Optional[float] = None
    reason: Optional[str] = None

    def to_dict(self) -> Dict:
        data = asdict(self)
        data['action'] = self.action.value
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict) -> 'ScheduleResponse':
        action = PlacementAction(data['action'])
        return cls(
            worker_id=data['worker_id'],
            action=action,
            estimated_wait_time=data.get('estimated_wait_time'),
            reason=data.get('reason')
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ScheduleResponse':
        return cls.from_dict(json.loads(json_str))


# Examples
EXAMPLE_WORKER_LOAD_REPORT = {
    "node_id": "w1",
    "models_loaded": ["m1", "m2"],
    "queue_depth": 3,
    "memory_utilization": 0.65,
    "is_ready": True,
    "timestamp": 1234567890.123
}

EXAMPLE_SCHEDULE_REQUEST = {
    "request_id": "r1",
    "model_required": "m3",
    "priority": 1
}

EXAMPLE_SCHEDULE_RESPONSE = {
    "worker_id": "wX",
    "action": "serve",
    "estimated_wait_time": 2.5,
    "reason": "Worker has model loaded with low queue depth"
}
