"""
Stateless client for sending schedule requests to the decentralized cluster.
"""

import socket
import pickle
import logging
from typing import List, Tuple, Optional
from multinode.contracts import ScheduleRequest, ScheduleResponse

logger = logging.getLogger("ClusterClient")


class ClusterClient:
    def __init__(self, entry_nodes: List[Tuple[str, int]], request_timeout: float = 2.0):
        if not entry_nodes:
            raise ValueError("At least one entry node must be provided")
        self.entry_nodes = entry_nodes
        self.request_timeout = request_timeout
        logger.info(f"Client initialized with entry nodes: {entry_nodes}")

    def request_schedule(self, request_id: str, model_required: str) -> Optional[ScheduleResponse]:
        request = ScheduleRequest(request_id=request_id, model_required=model_required)
        message = {'type': 'schedule_request', 'payload': request.to_dict()}
        serialized = pickle.dumps(message)

        target_addr = self.entry_nodes[0]

        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.request_timeout)

            sock.sendto(serialized, target_addr)

            raw_data, _ = sock.recvfrom(8192)
            response_msg = pickle.loads(raw_data)

            if response_msg.get('type') == 'schedule_response':
                return ScheduleResponse.from_dict(response_msg['payload'])

            if response_msg.get('type') == 'schedule_error':
                logger.error(f"Schedule error: {response_msg['payload'].get('error')}")
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
