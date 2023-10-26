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

import functools
import itertools
import random
import socket
import threading
import time

from concurrent.futures import ThreadPoolExecutor, wait as future_wait
from collections import defaultdict, namedtuple

from . import ClusterUtil
from .DaxError import DaxClientError, DaxServiceError, DaxErrorCode, DaxValidationError
from .Assemblers import ENDPOINT_FIELDS

import logging
logger = logging.getLogger(__name__)

SCHEMES_TO_DEFAULT_PORTS = { 'dax': 8111, 'daxs': 9111 }

_ServiceEndpoint = namedtuple('_ServiceEndpoint', ENDPOINT_FIELDS)
AddrPort = namedtuple('AddrPort', ('address', 'port'))

class ServiceEndpoint(_ServiceEndpoint):
    @classmethod
    def from_endpoint(cls, endpoint):
        # Set the hostname to the address if none provided
        if 'hostname' in endpoint:
            if 'address' not in endpoint:
                # No address, so go ask DNS
                addresses = _resolve_dns(endpoint['hostname'], endpoint['port'])
                if addresses:
                    # If multiple addresses, pick one
                    endpoint['address'] = random.choice(addresses)[0]
                else:
                    # Could not resolve hostname, bad endpoint, ignore it
                    return None
        else:
            if endpoint.setdefault('hostname', endpoint.get('address')) is None:
                # No hostname or address, go to the next endpoint
                return None

        endpoint.setdefault('leader_session_id', 0)

        return cls(**endpoint)

    @classmethod
    def from_endpoints(cls, endpoints):
        ''' Create a frozenset of ServiceEndpoint objects from a list of endpoints.

        Endpoints without valid addresses are dropped.
        '''
        service_endpoints = [cls.from_endpoint(endpoint) for endpoint in endpoints]
        return frozenset(service_endpoint for service_endpoint in service_endpoints if service_endpoint is not None)

    @property
    def addrport(self):
        try:
            return self._addrport
        except AttributeError:
            self._addrport = AddrPort(self.address, self.port) # pylint: disable=attribute-defined-outside-init
            return self._addrport

class Role(object):
    LEADER = 1
    REPLICA = 2

    @staticmethod
    def name(n):
        if n == Role.LEADER:
            return "LEADER"
        elif n == Role.REPLICA:
            return "REPLICA"
        else:
            raise ValueError("Unknown role %d" % n)

class Router(object):
    ''' Abstract class that determines how the cluster routes requests. '''
    def close(self):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def update(self, service_endpoints):
        raise NotImplementedError()

    def wait_for_routes(self, min_healthy=1, leader_min=1, timeout=None):
        raise NotImplementedError()

    def next_leader(self, prev_client):
        raise NotImplementedError()

    def next_any(self, prev_client):
        raise NotImplementedError()

class EndpointRouter(Router):
    ''' Determine which nodes to route requests to.

    This manages the EndpointBackend objects that represent nodes in the cluster, and allows retrieving the appropriate
    node type (leader or replica).
    '''
    def __init__(self,
                 client_factory,
                 discovery_endpoints,
                 update_cluster_interval,
                 health_check_interval,
                 selector=None,
                 executor=None):
        self.selector = selector or random_backend

        if len({scheme for scheme, _, _ in discovery_endpoints}) > 1:
            raise DaxValidationError('All endpoints must have the same scheme')
        self._discovery_endpoints = discovery_endpoints
        # set the scheme for the client factory
        self._client_factory = functools.partial(client_factory, discovery_endpoints[0][0])

        # endpoints returned from last successful endpoints() call to the cluster
        self._service_endpoints = frozenset()

        # backends is a dict[role, dict[(hostname,port,node), backend] of the
        # active backends
        self.backends = defaultdict(dict)

        # dict of AddrPort -> EndpointBackend of inactive backeends. When they
        # come up, they are moved to self.backends.
        self._pending_backends = {}

        self._health_check_interval = health_check_interval
        self._update_cluster_interval = update_cluster_interval

        self._lock = threading.RLock()
        self._route_change = threading.Condition(self._lock)

        # Executor for health checks
        self._background = executor or ThreadPoolExecutor(max_workers=2)

        # Thread that refreshes the roster
        self._refresh_task = None

    @property
    def all_backends(self):
        return list(self._all_backends())

    def _all_backends(self):
        return itertools.chain.from_iterable(role_backends.values() for role, role_backends in self.backends.items())

    @property
    def leader_backends(self):
        return list(self.backends[Role.LEADER].values())

    def close(self):
        with self._lock:
            if self._refresh_task:
                self._refresh_task.cancel()
                self._refresh_task = None

            backends = self.all_backends + list(self._pending_backends.values())

        closers = [self._background.submit(backend.close) for backend in backends]
        done, not_done = future_wait(closers, timeout=5.0) # pylint: disable=unused-variable
        # TODO What to do with not_done?

        with self._lock:
            self.backends.clear()
            self.backends = None

        self._background.shutdown()

    def update(self, service_endpoints):
        ''' Update the set of connected endpoints.

        If an endpoint has changed roles, move it. If it is new, create a new backend.
        If a backend is not in the new endpoint list, close & remove it.
        '''
        if self._service_endpoints == service_endpoints:
            # If the backends haven't changed, skip the update process
            return

        pending = []
        with self._lock:
            for ep in service_endpoints:
                self._update_endpoint(ep)

            pending.extend(self._purge_endpoints(service_endpoints))
            self._service_endpoints = service_endpoints

        done, not_done = future_wait(pending, timeout=5.0) # pylint: disable=unused-variable
        # TODO What to do with not_done?

        logger.debug('Current backends: %s', dict(self.backends))

    def _update_endpoint(self, service_endpoint):
        ''' Update any endpoints that have changed or are new.

        Not thread-safe. Must be called from inside update().
        '''
        new_addrport = service_endpoint.addrport
        new_role = service_endpoint.role

        backend = self.backends[new_role].get(new_addrport)
        if backend is not None:
            # Backend is up-to-date, no changes necessary
            logger.debug('Updating active backend %s', backend)
            backend.update(service_endpoint)
        else:
            # Role may have changed, so search other roles to see if we're already connected
            for role, role_backends in self.backends.items():
                if role == new_role:
                    # Already checked...
                    continue

                backend = role_backends.pop(new_addrport, None)
                if backend is not None:
                    # Backend has only changed roles, so just move it
                    backend.update(service_endpoint)
                    self.backends[new_role][new_addrport] = backend
                    logger.debug('%s changing role from %s to %s', backend, Role.name(role), Role.name(new_role))
                    break
            else: # no break
                # Update it if pending
                if new_addrport in self._pending_backends:
                    logger.debug('Updating pending backend %s', self._pending_backends[new_addrport])
                    self._pending_backends[new_addrport].update(service_endpoint)
                else:
                    # Backend does not exist, so create it
                    # It will add itself when it is ready
                    self._create_backend(service_endpoint)

    def _purge_endpoints(self, service_endpoints):
        ''' Remove backends that no longer have corresponding endpoints.

        Not thread-safe. Must be called from inside update().
        '''
        endpoints = {ep.addrport for ep in service_endpoints}

        pending = []
        for role_backends in self.backends.values():
            to_remove = []
            for addrport, backend in role_backends.items():
                if addrport not in endpoints:
                    # This backend is no longer in the service endpoints, so close it
                    # It will remove itself when closed
                    logger.debug('Removing unused backend %s', backend)
                    pending.append(self._background.submit(backend.close)) # Method ref, not call
                    to_remove.append(addrport) # Can't delete while looping

            # Remove all keys
            for addrport in to_remove:
                del role_backends[addrport]

        to_remove = []
        for addrport, backend in self._pending_backends.items():
            if addrport not in endpoints:
                logger.debug('Removing unused pending backend %s', addrport)
                pending.append(self._background.submit(backend.close))
                to_remove.append(addrport)

        for addrport in to_remove:
            del self._pending_backends[addrport]

        return pending

    def _create_backend(self, service_endpoint):
        ''' Create a new Backend instance and add it to the pending list.

        If a backend is already pending, do nothing.

        Not thread-safe. Must be called from inside update().
        '''

        logger.debug('Creating backend for %s', service_endpoint)

        pending_backend = self._pending_backends.get(service_endpoint.addrport)
        if pending_backend is None:
            logger.debug('Creating backend for %s', service_endpoint)
            pending_backend = EndpointBackend(
                service_endpoint,
                self._client_factory,
                self._health_check_interval,
                self._backend_up,
                self._backend_down)
            self._pending_backends[service_endpoint.addrport] = pending_backend

            # Bring the backend up in the background
            self._background.submit(pending_backend.start)
        else:
            # There is already a pending backend for this endpoint,
            # so let it continue instead
            pass

    def _backend_up(self, backend):
        with self._lock:
            self.backends[backend.role][backend.addrport] = backend
            self._pending_backends.pop(backend.addrport, None)
            self._route_change.notify_all()
        logger.debug('Backend up: %s', backend)

    def _backend_down(self, backend):
        role = backend.role
        with self._lock:
            # Move it to pending if it's being brought down because of a health
            # check or other failure. Otherwise it's been removed from the
            # roster.
            if next((e for e in self._service_endpoints if e.addrport == backend.addrport), False):
                self._pending_backends[backend.addrport] = backend
            self.backends[role].pop(backend.addrport, None)
            self._route_change.notify_all()
        logger.debug('Backend down: %s', backend)

    def wait_for_routes(self, min_healthy=1, leader_min=1, timeout=None):
        start = _clock()
        while True:
            with self._lock:
                if self._has_active_any(min_healthy) and self._has_active_leaders(leader_min):
                    return
                else:
                    now = _clock()
                    if timeout is not None and now - start > timeout:
                        raise DaxClientError(
                            "Not enough routes after {}s: expected {}/{}, found {}/{} (healthy/leaders)".format(
                                timeout, min_healthy, leader_min, self._active_any(), self._active_leaders()),
                            DaxErrorCode.NoRoute)

                # Wait for a signal that the endpoints have changed
                self._route_change.wait(timeout)

    def _active_any(self):
        return sum(len(role_backend) for role, role_backend in self.backends.items())

    def _has_active_any(self, min_healthy):
        if min_healthy < 1:
            return True
        else:
            return self._active_any() >= min_healthy

    def _active_leaders(self):
        return len(self.backends[Role.LEADER])

    def _has_active_leaders(self, leader_min):
        if leader_min < 1:
            return True
        else:
            return self._active_leaders() >= leader_min

    def next_leader(self, prev_client):
        ''' Returns the next leader entry that is not the given prev value if one such entry is available.

        If there is only one entry and that is equals to prev, prev is returned.
        Returns None if nothing is available.
        '''

        with self._lock:
            if self.backends is not None:
                backend = self.selector(prev_client, self.leader_backends)
                return backend.client if backend is not None else None

    def next_any(self, prev_client):
        ''' Returns any entry that is not the given prev value, if any such entry is available.

        If there is only one entry and that is equals to prev, prev is returned.
        Returns None if nothing is available.
        '''
        with self._lock:
            if self.backends is not None:
                backend = self.selector(prev_client, self.all_backends)
                return backend.client if backend is not None else None

    def start(self):
        ''' Set up the initial roster of nodes using the provided discovery endpoints. '''
        try:
            self._bootstrap()
        except DaxValidationError as e:
            logger.info("Could not start bootstrap due to DaxValidationError: %s", e)
            raise
        if not self._service_endpoints:
            raise DaxClientError('Failed to configure cluster endpoints from {}'.format(self._discovery_endpoints), DaxErrorCode.NoRoute)

        with self._lock:
            if self._refresh_task:
                self._refresh_task.cancel()

            self._refresh_task = ClusterUtil.periodic_task(self._bootstrap_async, self._update_cluster_interval, jitter=0.5)

    def _bootstrap_async(self):
        '''Non-throwing bootstrap, for calling from a background thread.'''

        try:
            self._bootstrap()
        except Exception as e: # pylint: disable=broad-except
            logger.warning('Failed to boostrap endpoints', exc_info=e)

    def _bootstrap(self):
        service_endpoints = self._bootstrap_endpoints()
        logger.debug('Bootstrapped endpoints: %s', service_endpoints)
        if service_endpoints:
            self.update(service_endpoints)

    def _bootstrap_endpoints(self):
        logger.debug("Bootstrapping from discovery endpoints %s", self._discovery_endpoints)

        seeds = []
        for scheme, host, port in self._discovery_endpoints:
            # Find all IP addresses for this hostname from DNS
            seeds_from_endpoint = _resolve_dns(host, port)
            logger.debug('Resolved addresses %s for %s', seeds_from_endpoint, host)
            seeds += seeds_from_endpoint
            endpoint_scheme = scheme # should be the same for all endpoints

        random.shuffle(seeds)
        logger.debug('seeds: %s', seeds)
        service_endpoints = self._get_endpoints_from_seeds(endpoint_scheme, seeds)
        logger.debug('service_endpoints: %s', service_endpoints)

        if service_endpoints:
            return service_endpoints

    def _get_endpoints_from_seeds(self, scheme, seeds):
        for hostname, sockaddr in seeds:
            ip, port = sockaddr
            # Stop after the first successful call
            try:
                client = self._client_factory(hostname, sockaddr)
            except Exception: # pylint: disable=broad-except
                logger.info('Could not connect to %s://%s:%s', scheme, ip,
                            port, exc_info=True)
                continue
            else:
                try:
                    endpoints = client.endpoints()

                    # Restore the DNS domain name of the discovery endpoint,
                    # for certificate validation
                    for endpoint in endpoints:
                        endpoint['hostname'] = hostname

                except DaxServiceError as e:
                    if e.auth_error:
                        # Startup will fail with authentication errors if region
                        # is not set correctly on client config. Region can also be
                        # set after instantiating cluster client. Ignore the exception.
                        logger.warning('Auth exception while starting up cluster client: %s', e)

                        # Re-raise because this will be unrecoverable
                        raise
                except DaxValidationError:
                    raise
                except Exception: # pylint: disable=broad-except
                    logger.info('Failed to retrieve endpoints from %s://%s:%s',
                                scheme, ip, port, exc_info=True)
                    continue
                else:
                    if endpoints:
                        return ServiceEndpoint.from_endpoints(endpoints)
                finally:
                    client.close()


def random_backend(prev_client, backends):
    ''' Select a random available client. '''
    if not backends:
        return None
    elif len(backends) == 1:
        return backends[0]
    else:
        # Select a random backend
        i = random.randrange(len(backends))
        be = backends[i]
        if be.client is prev_client:
            # Just pick the next one
            be = backends[(i + 1) % len(backends)]
        return be

class EndpointBackend(object):

    @staticmethod
    def _noop(*args, **kwargs):
        pass

    def __init__(self, service_endpoint, client_factory, health_check_interval, on_up, on_down):
        self._service_endpoint = service_endpoint
        self._client_factory = client_factory
        self._health_check_interval = health_check_interval
        self._connection_timeout = self._health_check_interval / 2

        self._on_up = on_up
        self._on_down = on_down

        self._error_count = 0

        self.client = None
        self.session = None
        self.active = False
        self._healthy = True  # Healthy until proven otherwise
        self._closed = False

        self._lock = threading.RLock()
        self._timer = None

    def start(self):
        self._health_check()
        logger.debug('Creating health checker for %s', self)
        self._timer = ClusterUtil.periodic_task(
            self._health_check,
            self._health_check_interval,
            self._health_check_interval * 0.1)

    def update(self, new_service_endpoint):
        if self._service_endpoint != new_service_endpoint:
            if self._service_endpoint.addrport != new_service_endpoint.addrport:
                raise ValueError('Cannot update backend to new address.')

            self._service_endpoint = new_service_endpoint
            return True

        return False

    def close(self):
        with self._lock:
            if self._closed:
                return

            logger.debug('Closing %s', self)

            if self._timer:
                self._timer.cancel()
                self._timer = None

            if self.active:
                self.down()

            self._closed = True

    @property
    def addrport(self):
        return self._service_endpoint.addrport

    @property
    def role(self):
        return self._service_endpoint.role

    @property
    def leader(self):
        return self._service_endpoint.role == Role.LEADER

    @property
    def suspect(self):
        return self._error_count > 0

    def up(self):
        upped = False
        with self._lock:
            if not self.active:
                self._error_count = 0
                self.active = True
                upped = True

        if upped:
            self._on_up(self)

    def down(self):
        if self._closed:
            return

        self._on_down(self)

        with self._lock:
            self.active = False
            if self.client:
                self.client.close()
                self.client = None

    def _health_check(self):
        with self._lock:
            if self._closed:
                return

            if not self.client:
                try:
                    self.client = self._client_factory(self._service_endpoint.hostname, self.addrport)
                except Exception as e: # pylint: disable=broad-except
                    logger.warning("Health check failed to get client with Error %s", e)

            # Determine health by calling endpoints
            try:
                logger.debug('Running health check for %s', self)
                _ = self.client.endpoints()
            except Exception as e: # pylint: disable=broad-except
                logger.warning("Health check failed for %s: %s", self.addrport.address, e)
                if self.active:
                    self.down()

                return
            else:
                if not self.active:
                    self.up()

        return

    def __repr__(self):
        return 'EndpointBackend({})'.format(self._service_endpoint)

def _clock():
    return time.time()

def _resolve_dns(host, port):
    try:
        # Deliberately restrict it to IPv4 addresses
        return [(host, sockaddr) \
                for family, socktype, proto, canonname, sockaddr \
                in socket.getaddrinfo(host, port, socket.AF_INET, 0, socket.IPPROTO_TCP)]
    except socket.gaierror:
        # if there is an error, return no addresses
        return []
