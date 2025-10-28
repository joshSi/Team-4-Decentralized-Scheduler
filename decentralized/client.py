"""
Stateless client for sending schedule requests to the decentralized cluster.
"""

import socket
import pickle
import time
import random
import logging
from typing import List, Tuple, Optional
from contracts import ScheduleRequest, ScheduleResponse

logger = logging.getLogger("ClusterClient")


class ClusterClient:
    """
    A simple, stateless client to interact with the cluster.

    It sends a schedule request to an entry node and waits for a response.
    It uses a new (ephemeral) socket for each request to avoid
    conflicts with listener threads.
    """

    def __init__(
        self,
        entry_nodes: List[Tuple[str, int]],
        request_timeout: float = 2.0
    ):
        """
        Initialize the client.

        Args:
            entry_nodes: List of (host, port) tuples of cluster nodes
            request_timeout: Timeout for waiting for a response
        """
        if not entry_nodes:
            raise ValueError("At least one entry node must be provided")
        self.entry_nodes = entry_nodes
        self.request_timeout = request_timeout
        logger.info(f"Client initialized with entry nodes: {entry_nodes}")

    def request_schedule(
        self,
        request_id: str,
        model_required: str
    ) -> Optional[ScheduleResponse]:
        """
        Send a schedule request and wait for a response.

        Args:
            request_id: Unique identifier for the request
            model_required: Model needed for inference

        Returns:
            ScheduleResponse if successful, None if timeout or error
        """
        request = ScheduleRequest(
            request_id=request_id,
            model_required=model_required
        )
        message = {
            'type': 'schedule_request',
            'payload': request.to_dict()
        }
        serialized = pickle.dumps(message)

        # Pick a random entry node
        target_addr = random.choice(self.entry_nodes)
        
        sock = None
        try:
            # Create a new socket for each request
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.request_timeout)
            
            # Send request
            sock.sendto(serialized, target_addr)

            # Wait for response
            raw_data, _ = sock.recvfrom(8192)
            response_msg = pickle.loads(raw_data)

            if response_msg.get('type') == 'schedule_response':
                response = ScheduleResponse.from_dict(response_msg['payload'])
                logger.debug(
                    f"Schedule response for {request_id}: "
                    f"worker={response.worker_id}, action={response.action.value}"
                )
                return response
                
            elif response_msg.get('type') == 'schedule_error':
                logger.error(
                    f"Schedule error: {response_msg['payload'].get('error')}"
                )
                return None

        except socket.timeout:
            logger.warning(f"Timeout waiting for schedule response for {request_id}")
            return None
        except (socket.error, pickle.UnpicklingError, EOFError) as e:
            logger.error(f"Error requesting schedule: {e}", exc_info=True)
            return None
        finally:
            if sock:
                sock.close()
                
        return None
