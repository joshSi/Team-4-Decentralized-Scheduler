# multinode/__init__.py
"""
Multinode: lightweight, gossip-based, decentralized scheduler + loadgen.

This package exposes core building blocks:
- DecentralizedNode: a peer that gossips cluster state and makes local scheduling decisions
- ClusterClient: a stateless UDP client for schedule requests
- DatasetLoader: helpers for GSM8K / ShareGPT workloads
- WorkloadGenerator, RequestTrace: bursty request trace generation (Gamma, CV≈8)
- Contracts: dataclasses for on-wire messages and scheduling actions
"""

__all__ = [
    "DecentralizedNode",
    "ClusterClient",
    "DatasetLoader",
    "WorkloadGenerator",
    "RequestTrace",
    "ScheduleRequest",
    "ScheduleResponse",
    "WorkerLoadReport",
    "PlacementAction",
    "__version__",
]

__version__ = "0.1.0"

# Re-export convenient symbols from submodules
from .decentralized.decentralized_node import DecentralizedNode
from .decentralized.client import ClusterClient
from .decentralized.dataset_loader import DatasetLoader
from .decentralized.workload_generator import WorkloadGenerator, RequestTrace
from .decentralized.contracts import (
    ScheduleRequest,
    ScheduleResponse,
    WorkerLoadReport,
    PlacementAction,
)