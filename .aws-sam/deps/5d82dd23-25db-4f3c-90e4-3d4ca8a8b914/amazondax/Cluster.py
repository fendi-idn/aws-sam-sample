# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import threading
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from .Tube import SocketTubePool
from .DaxClient import DaxClient
from .DaxError import DaxClientError, DaxErrorCode, DaxValidationError
from .Router import SCHEMES_TO_DEFAULT_PORTS, EndpointRouter

import logging
logger = logging.getLogger(__name__)

class Cluster(object):
    def __init__(self, region_name, discovery_endpoints, credentials, user_agent=None, user_agent_extra=None, connect_timeout=None,
                 read_timeout=None, skip_hostname_verification=None, router_factory=None, client_factory=None):
        self._region_name = region_name
        self._discovery_endpoints = [_parse_host_ports(endpoint) for endpoint in discovery_endpoints]
        self._credentials = credentials
        self._user_agent = user_agent
        self._user_agent_extra = user_agent_extra
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._client_factory = client_factory or self._new_client
        self._skip_hostname_verification = skip_hostname_verification

        _router_factory = router_factory or EndpointRouter

        self._closed = False

        # All time intervals are in seconds to match time.time
        self._cluster_update_interval = 4.0
        self._health_check_interval = 5.0
        self._idle_connection_reap_delay = 30.0

        self._router = _router_factory(self._client_factory,
                                       self._discovery_endpoints,
                                       self._cluster_update_interval,
                                       self._health_check_interval)

        self._lock = threading.Lock()

    def start(self, min_healthy=1):
        self._router.start()
        self.wait_for_routes(min_healthy=min_healthy, leader_min=1, timeout=self._connect_timeout)

    def close(self):
        if self._closed:
            return

        self._closed = True

        try:
            self._router.close()
        except Exception as e: # pylint: disable=broad-except
            logger.warning('Failed closing router', exec_info=e)
        finally:
            self._router = None

    def read_client(self, prev_client=None):
        ''' Return a read client.

        Caller should not close the client.
        '''
        client = self._router.next_any(prev_client)
        if client is None:
            raise DaxClientError("No cluster endpoints available", DaxErrorCode.NoRoute)

        return client

    def write_client(self, prev_client=None):
        ''' Return a write client.

        Caller should not close the client.
        '''
        client = self._router.next_leader(prev_client)
        if client is None:
            raise DaxClientError("No cluster endpoints available", DaxErrorCode.NoRoute)

        return client

    def wait_for_routes(self, min_healthy=1, leader_min=1, timeout=None):
        return self._router.wait_for_routes(min_healthy=min_healthy, leader_min=leader_min, timeout=timeout)

    def _new_client(self, scheme, hostname, sockaddr):
        ''' Create a new client.

        Caller is responsible for closing the client.
        '''
        tube_pool = SocketTubePool(
                scheme, hostname, sockaddr,
                lambda: self._credentials,
                self._region_name,
                self._user_agent,
                self._user_agent_extra,
                self._connect_timeout,
                self._read_timeout,
                self._skip_hostname_verification)
        return DaxClient(tube_pool)

def _parse_host_ports(endpoint):
    # We accept endpoints in the form hostname:port. We also call these
    # endpoint_urls, although that is not a valid URL (the part before the
    # colon should be the scheme. Deal with that here by turning a non-URL
    # endpoint into a URL. Assume that the non-URL form represents a
    # non-encrypted URL.
    parts = urlparse(endpoint if '://' in endpoint else 'dax://' + endpoint)
    if parts.scheme not in SCHEMES_TO_DEFAULT_PORTS.keys():
        raise DaxValidationError('URL scheme must be one of {}'.format(
            ', '.join(SCHEMES_TO_DEFAULT_PORTS.keys())))

    netloc_parts = parts.netloc.split(':', 1)
    if len(netloc_parts) == 1:
        return parts.scheme, netloc_parts[0].strip(), SCHEMES_TO_DEFAULT_PORTS[parts.scheme]
    else:
        return parts.scheme, netloc_parts[0].strip(), int(netloc_parts[1].strip())
