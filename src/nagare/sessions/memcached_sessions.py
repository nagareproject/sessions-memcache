# --
# Copyright (c) 2008-2023 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

from hashlib import md5

from nagare.sessions import common
from nagare.sessions.exceptions import ExpirationError, StorageError

KEY_PREFIX = 'nagare_%d_'


class Sessions(common.Sessions):
    """Sessions manager for sessions kept in an external memcached server."""

    CONFIG_SPEC = dict(
        common.Sessions.CONFIG_SPEC,
        ttl='integer(default=0)',
        lock_ttl='float(default=0.)',
        lock_poll_time='float(default=0.1)',
        lock_max_wait_time='float(default=5.)',
        noreply='boolean(default=False)',
        reset_on_reload='option(on, off, invalidate, flush, default="invalidate")',
        version='string(default="")',
        serializer='string(default="nagare.sessions.serializer:Pickle")',
    )

    def __init__(
        self,
        name,
        dist,
        ttl=0,
        lock_ttl=0,
        lock_poll_time=0.1,
        lock_max_wait_time=5,
        noreply=False,
        reset_on_reload='invalidate',
        version='',
        serializer='nagare.sessions.serializer:Pickle',
        memcache_service=None,
        services_service=None,
        **config,
    ):
        """Initialization.

        In:
          - ``ttl`` -- sessions and continuations timeout, in seconds (0 = no timeout)
          - ``lock_ttl`` -- session locks timeout, in seconds (0 = no timeout)
          - ``lock_poll_time`` -- wait time between two lock acquisition tries, in seconds
          - ``lock_max_wait_time`` -- maximum time to wait to acquire the lock, in seconds
          - ``serializer`` -- serializer / deserializer of the states
        """
        services_service(
            super(Sessions, self).__init__,
            name,
            dist,
            ttl=ttl,
            lock_ttl=lock_ttl,
            lock_poll_time=lock_poll_time,
            lock_max_wait_time=lock_max_wait_time,
            noreply=noreply,
            reset_on_reload=reset_on_reload,
            version=version,
            serializer=serializer,
            **config,
        )

        self.ttl = ttl
        self.lock_ttl = lock_ttl
        self.lock_poll_time = lock_poll_time
        self.lock_max_wait_time = lock_max_wait_time
        self.memcache = memcache_service
        self.noreply = noreply

        self.reset_on_reload = 'invalidate' if reset_on_reload == 'on' else reset_on_reload
        self.version = self._version = version

        self.handle_reload()

    def generate_version(self):
        return str(self.generate_id())

    def handle_reload(self):
        if self.reset_on_reload == 'invalidate':
            self.version = md5((self._version or self.generate_version()).encode('utf-8')).hexdigest()[:16]
            self.logger.info("Sessions version '{}'".format(self.version))

        if self.reset_on_reload == 'flush':
            self.memcache.flush_all()
            self.logger.info('Deleting all the sessions')

    def check_concurrence(self, multi_processes, multi_threads):
        pass

    def check_session_id(self, session_id):
        return False

    def get_lock(self, session_id):
        return self.memcache.get_lock(session_id, self.lock_ttl, self.lock_poll_time, self.lock_max_wait_time)

    def _create(self, session_id, secure_id):
        """Create a new session.

        Return:
          - id of the session
          - id of the state
          - secure token associated to the session
          - session lock
        """
        if self.memcache.set_multi(
            {'state': 0, 'sess': (self.version, secure_id, None), '00000': ''},
            self.ttl,
            KEY_PREFIX % session_id,
            noreply=self.noreply,
        ):
            raise StorageError("can't create memcache session {}".format(session_id))

        return session_id, 0, secure_id, self.get_lock(session_id)

    def delete(self, session_id):
        """Delete a session.

        In:
          - ``session_id`` -- id of the session to delete
        """
        if self.memcache.delete((KEY_PREFIX + 'sess') % session_id, self.noreply):
            raise StorageError("can't delete memcache session {}".format(session_id))

    def _fetch(self, session_id, state_id):
        """Retrieve a state with its associated objects graph.

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
            raise ExpirationError('session not found')

        last_state_id = session['state']
        version, secure_token, session_data = session['sess']
        state_data = session[state_id]

        if version != self.version:
            raise ExpirationError('invalid session version')

        return last_state_id, secure_token, session_data, state_data

    def _store(self, session_id, state_id, secure_token, use_same_state, session_data, state_data):
        """Store a state and its associated objects graph.

        In:
          - ``session_id`` -- session id of this state
          - ``state_id`` -- id of this state
          - ``secure_id`` -- the secure number associated to the session
          - ``use_same_state`` -- is this state to be stored in the previous snapshot?
          - ``session_data`` -- data to keep into the session
          - ``state_data`` -- data to keep into the state
        """
        if (
            not use_same_state
            and self.memcache.incr((KEY_PREFIX + 'state') % session_id, noreply=self.noreply) is None
            and not self.noreply
        ):
            raise StorageError("can't store memcache state in session {}, state {}".format(session_id, state_id))

        if self.memcache.set_multi(
            {'sess': (self.version, secure_token, session_data), '%05d' % state_id: state_data},
            self.ttl,
            KEY_PREFIX % session_id,
            noreply=self.noreply,
        ):
            raise StorageError("can't store memcache data in session {}, state {}".format(session_id, state_id))
