"""
Data contracts for the centralized scheduler and worker coordination.

This module defines the data structures used for communication between workers
and the centralized coordinator in the serverless LLM cluster.
"""

from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from enum import Enum

class PlacementAction(Enum):
    """Actions that can be taken when placing a request."""
    SERVE = "serve"
    COLD_START = "cold_start"
    MIGRATE = "migrate"


@dataclass
class WorkerLoadReport:
    """
    Represents the current state of a worker node.

    This is sent periodically by workers to the centralized coordinator
    to maintain a real-time view of cluster state.
    """
    node_id: str                         # Unique identifier for the worker node
    models_loaded: List[str]             # Models currently loaded in memory  
    queue_depth: int                     # Number of pending requests
    memory_utilization: float            # Memory utilization as a fraction (0.0 to 1.0)
    is_ready: bool = False               # Whether the worker is ready for inference
    timestamp: Optional[float] = None    # Unix timestamp of the report

    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict):
        """Create from dictionary."""
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str):
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def is_overloaded(self, queue_threshold: int = 10, memory_threshold: float = 0.9) -> bool:
        """Check if worker is overloaded based on thresholds."""
        return self.queue_depth > queue_threshold or self.memory_utilization > memory_threshold


@dataclass
class ScheduleRequest:
    """
    Request to schedule an inference task.

    Sent by an entry node to the centralized coordinator to find the best
    worker for serving an inference request.

    Attributes:
        request_id: Unique identifier for this inference request
        model_required: Model identifier needed for inference
        priority: Optional priority level (higher is more urgent)
    """
    request_id: str
    model_required: str
    priority: Optional[int] = 0

    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict) -> 'ScheduleRequest':
        """Create from dictionary."""
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> 'ScheduleRequest':
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class ScheduleResponse:
    """
    Response from the centralized coordinator with placement decision.

    Attributes:
        worker_id: Identifier of the worker node that should handle the request
        action: The action to take (SERVE, COLD_START, or MIGRATE)
        estimated_wait_time: Optional estimated time until processing can begin (seconds)
        reason: Optional explanation for the placement decision
    """
    worker_id: str
    action: PlacementAction
    estimated_wait_time: Optional[float] = None
    reason: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        data = asdict(self)
        data['action'] = self.action.value
        return data

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict) -> 'ScheduleResponse':
        """Create from dictionary."""
        action = PlacementAction(data['action'])
        return cls(
            worker_id=data['worker_id'],
            action=action,
            estimated_wait_time=data.get('estimated_wait_time'),
            reason=data.get('reason')
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ScheduleResponse':
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


# Example JSON formats for reference
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

