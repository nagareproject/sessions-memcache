# --
# Copyright (c) 2008-2021 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

from nagare.sessions import common
from nagare.sessions.exceptions import StorageError, ExpirationError

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
        noreply='boolean(default=False)',
        serializer='string(default="nagare.sessions.serializer:Pickle")'
    )

    def __init__(
            self,
            name, dist,
            ttl=0,
            lock_ttl=0, lock_poll_time=0.1, lock_max_wait_time=5,
            min_compress_len=0, noreply=False,
            serialize=None,
            memcache_service=None, services_service=None,
            **config
    ):
        """Initialization

        In:
          - ``ttl`` -- sessions and continuations timeout, in seconds (0 = no timeout)
          - ``lock_ttl`` -- session locks timeout, in seconds (0 = no timeout)
          - ``lock_poll_time`` -- wait time between two lock acquisition tries, in seconds
          - ``lock_max_wait_time`` -- maximum time to wait to acquire the lock, in seconds
          - ``min_compress_len`` -- data longer than this value are sent compressed
          - ``serializer`` -- serializer / deserializer of the states
        """
        services_service(
            super(Sessions, self).__init__, name, dist,
            ttl=ttl,
            lock_ttl=lock_ttl, lock_poll_time=lock_poll_time, lock_max_wait_time=lock_max_wait_time,
            min_compress_len=min_compress_len, noreply=noreply,
            serialize=serialize,
            **config
        )

        self.ttl = ttl
        self.lock_ttl = lock_ttl
        self.lock_poll_time = lock_poll_time
        self.lock_max_wait_time = lock_max_wait_time
        self.memcache = memcache_service
        self.min_compress_len = min_compress_len
        self.noreply = noreply

        self.reload()

    def generate_version_id(self):
        return self.generate_id()

    def reload(self):
        self.version = self.generate_version_id()

    def check_concurrence(self, multi_processes, multi_threads):
        pass

    def check_session_id(self, session_id):
        return False

    def get_lock(self, session_id):
        return self.memcache.get_lock(
            session_id,
            self.lock_ttl, self.lock_poll_time, self.lock_max_wait_time,
            self.noreply
        )

    def _create(self, session_id, secure_id):
        """Create a new session

        Return:
          - id of the session
          - id of the state
          - secure token associated to the session
          - session lock
        """
        if self.memcache.set_multi({
            'state': 0,
            'sess': (self.version, secure_id, None),
            '00000': '',
        }, self.ttl, KEY_PREFIX % session_id, self.min_compress_len, self.noreply):
            raise StorageError('Memcache create session {}'.format(session_id))

        return session_id, 0, secure_id, self.get_lock(session_id)

    def delete(self, session_id):
        """Delete a session

        In:
          - ``session_id`` -- id of the session to delete
        """
        if self.memcache.delete((KEY_PREFIX + 'sess') % session_id, self.noreply):
            raise StorageError('Memcache delete session {}'.format(session_id))

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
        version, secure_token, session_data = session['sess']
        state_data = session[state_id]

        if version != self.version:
            raise ExpirationError()

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
            if self.memcache.incr((KEY_PREFIX + 'state') % session_id, noreply=self.noreply) is None:
                raise StorageError('Memcache store session {}, state {}'.format(session_id, state_id))

        if self.memcache.set_multi({
            'sess': (self.version, secure_token, session_data),
            '%05d' % state_id: state_data
        }, self.ttl, KEY_PREFIX % session_id, self.min_compress_len, self.noreply):
            raise StorageError('Memcache store session {}, state {}'.format(session_id, state_id))
