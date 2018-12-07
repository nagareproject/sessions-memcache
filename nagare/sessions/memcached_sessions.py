# --
# Copyright (c) 2008-2018 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

from nagare.sessions import common
from nagare.sessions.exceptions import ExpirationError

KEY_PREFIX = 'nagare_%d_'


class Sessions(common.Sessions):
    """Sessions manager for sessions kept in an external memcached server
    """
    CONFIG_SPEC = dict(
        common.Sessions.CONFIG_SPEC,
        ttl='integer(default=0)',
        lock_ttl='float(default=0.)',
        lock_poll_time='float(default=0.1)',
        lock_max_wait_time='float(default=5.)',
        min_compress_len='integer(default=0)',
        reset='boolean(default=True)',
        serializer='string(default="nagare.sessions.serializer:Pickle")'
    )

    def __init__(
            self,
            name, dist,
            ttl=0,
            lock_ttl=0, lock_poll_time=0.1, lock_max_wait_time=5,
            min_compress_len=0, reset=False,
            serialize=None,
            memcache_service=None,
            services_service=None,
            **config
    ):
        """Initialization

        In:
          - ``ttl`` -- sessions and continuations timeout, in seconds (0 = no timeout)
          - ``lock_ttl`` -- session locks timeout, in seconds (0 = no timeout)
          - ``lock_poll_time`` -- wait time between two lock acquisition tries, in seconds
          - ``lock_max_wait_time`` -- maximum time to wait to acquire the lock, in seconds
          - ``min_compress_len`` -- data longer than this value are sent compressed
          - ``reset`` -- do a reset of all the sessions on startup ?
          - ``serializer`` -- serializer / deserializer of the states
        """
        services_service(super(Sessions, self).__init__, name, dist, **config)

        self.ttl = ttl
        self.lock_ttl = lock_ttl
        self.lock_poll_time = lock_poll_time
        self.lock_max_wait_time = lock_max_wait_time
        self.memcache = memcache_service
        self.min_compress_len = min_compress_len

        if reset:
            self.flush_all()

    def flush_all(self):
        """Delete all the contents in the memcached server
        """
        self.memcache.flush_all()

    def check_concurrence(self, multi_processes, multi_threads):
        return

    def check_session_id(self, session_id):
        return False

    def get_lock(self, session_id):
        return self.memcache.get_lock(session_id, self.lock_ttl, self.lock_poll_time, self.lock_max_wait_time)

    def _create(self, session_id, secure_id):
        """Create a new session

        Return:
          - id of the session
          - id of the state
          - secure token associated to the session
          - session lock
        """
        self.memcache.set_multi({
            'state': 0,
            'sess': (secure_id, None),
            '00000': {}
        }, self.ttl, KEY_PREFIX % session_id, self.min_compress_len)

        return session_id, 0, secure_id, self.get_lock(session_id)

    def delete(self, session_id):
        """Delete a session

        In:
          - ``session_id`` -- id of the session to delete
        """
        self.memcache.delete((KEY_PREFIX + 'sess') % session_id)

    def _fetch(self, session_id, state_id):
        """Retrieve a state with its associated objects graph

        In:
          - ``session_id`` -- session id of this state
          - ``state_id`` -- id of this state

        Return:
          - id of the latest state
          - secure number associated to the session
          - data kept into the session
          - data kept into the state
        """
        state_id = '%05d' % state_id
        session = self.memcache.get_multi(('state', 'sess', state_id), KEY_PREFIX % session_id)

        if len(session) != 3:
            raise ExpirationError()

        last_state_id = session['state']
        secure_token, session_data = session['sess']
        state_data = session[state_id]

        return last_state_id, secure_token, session_data, state_data

    def _store(self, session_id, state_id, secure_token, use_same_state, session_data, state_data):
        """Store a state and its associated objects graph

        In:
          - ``session_id`` -- session id of this state
          - ``state_id`` -- id of this state
          - ``secure_id`` -- the secure number associated to the session
          - ``use_same_state`` -- is this state to be stored in the previous snapshot?
          - ``session_data`` -- data to keep into the session
          - ``state_data`` -- data to keep into the state
        """
        if not use_same_state:
            self.memcache.incr((KEY_PREFIX + 'state') % session_id)

        self.memcache.set_multi({
            'sess': (secure_token, session_data),
            '%05d' % state_id: state_data
        }, self.ttl, KEY_PREFIX % session_id, self.min_compress_len)