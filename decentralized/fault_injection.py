"""
Fault injection controller for testing node failures and recoveries.

Supports:
- Scheduled failures at specific times
- Automatic restarts after downtime
- Multiple failure/restart cycles
- Different failure modes (clean shutdown, crash, network partition)
"""

import time
import threading
import logging
from typing import List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("FaultInjection")


class FailureMode(Enum):
    """Types of failures that can be injected."""
    CLEAN_SHUTDOWN = "clean_shutdown"  # Graceful stop
    CRASH = "crash"  # Abrupt stop without cleanup
    NETWORK_PARTITION = "network_partition"  # Stop network but keep processing
    PAUSE = "pause"  # Pause all activity but maintain state


@dataclass
class FaultEvent:
    """Represents a single fault injection event."""
    time_offset: float  # Seconds after start to trigger
    mode: FailureMode
    duration: float  # How long to stay failed (seconds)
    
    def __str__(self):
        return f"{self.mode.value} at t+{self.time_offset}s for {self.duration}s"


class FaultInjectionController:
    """
    Controls fault injection for a decentralized node.
    
    Usage:
        controller = FaultInjectionController(node)
        controller.add_fault(time_offset=30.0, mode=FailureMode.CLEAN_SHUTDOWN, duration=10.0)
        controller.start()
    """
    
    def __init__(
        self,
        node,  # DecentralizedNode instance
        verbose: bool = True
    ):
        """
        Initialize fault injection controller.
        
        Args:
            node: The DecentralizedNode to control
            verbose: Enable detailed logging
        """
        self.node = node
        self.verbose = verbose
        self.faults: List[FaultEvent] = []
        
        self.is_running = False
        self.start_time = None
        self._controller_thread = None
        self._lock = threading.Lock()
        
        # Track node state
        self.is_node_running = False
        self.failure_count = 0
        self.recovery_count = 0
        
        logger.info("Fault injection controller initialized")
    
    def add_fault(
        self,
        time_offset: float,
        mode: FailureMode = FailureMode.CLEAN_SHUTDOWN,
        duration: float = 10.0
    ):
        """
        Schedule a fault injection event.
        
        Args:
            time_offset: Seconds after controller start to trigger fault
            mode: Type of failure to inject
            duration: How long the node should stay failed
        """
        fault = FaultEvent(time_offset, mode, duration)
        self.faults.append(fault)
        self.faults.sort(key=lambda f: f.time_offset)
        
        logger.info(f"Scheduled fault: {fault}")
        return fault
    
    def add_fault_schedule(self, schedule: str):
        """
        Parse and add faults from a schedule string.
        
        Format: "time:mode:duration,time:mode:duration,..."
        Example: "30:clean_shutdown:10,60:crash:15"
        
        Args:
            schedule: Comma-separated fault specifications
        """
        if not schedule:
            return
        
        for spec in schedule.split(','):
            parts = spec.strip().split(':')
            if len(parts) != 3:
                logger.warning(f"Invalid fault spec: {spec}")
                continue
            
            try:
                time_offset = float(parts[0])
                mode = FailureMode(parts[1])
                duration = float(parts[2])
                self.add_fault(time_offset, mode, duration)
            except (ValueError, KeyError) as e:
                logger.warning(f"Failed to parse fault spec '{spec}': {e}")
    
    def _inject_failure(self, fault: FaultEvent):
        """Execute a failure injection."""
        logger.warning(f"🔥 INJECTING FAULT: {fault}")
        print(f"METRIC_FAULT_INJECTION:{self.node.node_id},{fault.mode.value},{time.time()}", flush=True)
        
        self.failure_count += 1
        
        if fault.mode == FailureMode.CLEAN_SHUTDOWN:
            # Graceful shutdown
            logger.info(f"Performing clean shutdown of {self.node.node_id}")
            self.node.stop()
            self.is_node_running = False
            
        elif fault.mode == FailureMode.CRASH:
            # Abrupt stop - just kill the threads
            logger.info(f"Crashing {self.node.node_id} (abrupt stop)")
            self.node.is_running = False
            try:
                self.node.sock.close()
            except:
                pass
            self.is_node_running = False
            
        elif fault.mode == FailureMode.NETWORK_PARTITION:
            # Simulate network partition by closing socket
            logger.info(f"Simulating network partition for {self.node.node_id}")
            try:
                self.node.sock.close()
            except:
                pass
            # Node keeps running but can't communicate
            
        elif fault.mode == FailureMode.PAUSE:
            # Pause gossip and scheduling (more complex, requires node cooperation)
            logger.info(f"Pausing {self.node.node_id}")
            self.node.is_running = False
    
    def _recover_from_failure(self, fault: FaultEvent):
        """Recover from a failure injection."""
        logger.warning(f"♻️  RECOVERING FROM FAULT: {fault.mode.value}")
        print(f"METRIC_FAULT_RECOVERY:{self.node.node_id},{fault.mode.value},{time.time()}", flush=True)
        
        self.recovery_count += 1
        
        if fault.mode in [FailureMode.CLEAN_SHUTDOWN, FailureMode.CRASH]:
            # Need to restart the node
            logger.info(f"Restarting {self.node.node_id}")
            
            # Recreate socket
            import socket
            self.node.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.node.is_running = True
            
            # Restart the node
            self.node.start()
            self.is_node_running = True
            logger.info(f"Node {self.node.node_id} restarted successfully")
            
        elif fault.mode == FailureMode.NETWORK_PARTITION:
            # Reconnect to network
            logger.info(f"Reconnecting {self.node.node_id} to network")
            import socket
            self.node.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.node.sock.bind(self.node.addr)
                logger.info(f"Network reconnected for {self.node.node_id}")
            except OSError as e:
                logger.error(f"Failed to rebind socket: {e}")
                # Try with SO_REUSEADDR
                self.node.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.node.sock.bind(self.node.addr)
                
        elif fault.mode == FailureMode.PAUSE:
            # Resume operations
            logger.info(f"Resuming {self.node.node_id}")
            self.node.is_running = True
    
    def _controller_loop(self):
        """Main loop that monitors time and triggers faults."""
        logger.info("Fault injection controller started")
        self.start_time = time.time()
        self.is_node_running = True
        
        fault_index = 0
        
        while self.is_running:
            time.sleep(0.1)  # Check every 100ms
            
            if fault_index >= len(self.faults):
                # All faults processed
                continue
            
            current_fault = self.faults[fault_index]
            elapsed = time.time() - self.start_time
            
            # Check if it's time to inject the fault
            if elapsed >= current_fault.time_offset and self.is_node_running:
                self._inject_failure(current_fault)
                
                # Schedule recovery
                recovery_time = current_fault.time_offset + current_fault.duration
                
                # Wait for recovery time
                while time.time() - self.start_time < recovery_time and self.is_running:
                    time.sleep(0.1)
                
                if self.is_running:
                    self._recover_from_failure(current_fault)
                
                fault_index += 1
        
        logger.info("Fault injection controller stopped")
    
    def start(self):
        """Start the fault injection controller."""
        if self.is_running:
            logger.warning("Controller already running")
            return
        
        if not self.faults:
            logger.warning("No faults scheduled, controller will do nothing")
        
        self.is_running = True
        self._controller_thread = threading.Thread(
            target=self._controller_loop,
            name="FaultInjectionController"
        )
        self._controller_thread.start()
        
        logger.info(f"Started fault injection with {len(self.faults)} scheduled faults")
    
    def stop(self):
        """Stop the fault injection controller."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self._controller_thread:
            self._controller_thread.join()
        
        logger.info(
            f"Fault injection stopped. "
            f"Failures: {self.failure_count}, Recoveries: {self.recovery_count}"
        )
    
    def get_stats(self):
        """Get fault injection statistics."""
        return {
            'faults_scheduled': len(self.faults),
            'failures_injected': self.failure_count,
            'recoveries_completed': self.recovery_count,
            'is_running': self.is_running,
            'node_running': self.is_node_running
        }


def parse_fault_schedule_from_file(filepath: str) -> List[FaultEvent]:
    """
    Parse fault schedule from a configuration file.
    
    File format (one fault per line):
    # Comments start with #
    time_offset mode duration
    30 clean_shutdown 10
    60 crash 15
    
    Args:
        filepath: Path to configuration file
        
    Returns:
        List of FaultEvent objects
    """
    faults = []
    try:
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split()
                if len(parts) != 3:
                    logger.warning(f"Line {line_num}: Invalid format: {line}")
                    continue
                
                try:
                    time_offset = float(parts[0])
                    mode = FailureMode(parts[1])
                    duration = float(parts[2])
                    faults.append(FaultEvent(time_offset, mode, duration))
                except (ValueError, KeyError) as e:
                    logger.warning(f"Line {line_num}: Failed to parse: {e}")
        
        logger.info(f"Loaded {len(faults)} faults from {filepath}")
    except FileNotFoundError:
        logger.error(f"Fault schedule file not found: {filepath}")
    except Exception as e:
        logger.error(f"Error reading fault schedule: {e}")
    
    return faults
