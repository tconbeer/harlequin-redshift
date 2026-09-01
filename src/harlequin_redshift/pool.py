"""A small connection pool for ``redshift_connector``.

``redshift_connector`` declares ``threadsafety = 1``: threads may share the
module, but not a connection. Harlequin loads catalog items, completions, and
search results on worker threads while a query may be running on the main
connection, so those callers each need a connection of their own. The driver
ships no pool, so this is it -- deliberately small: connections are opened on
demand up to ``max_size``, handed out one at a time, and returned to an idle
list that later borrowers reuse.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator

import redshift_connector
from redshift_connector import Connection


class PoolClosedError(RuntimeError):
    """A connection was requested from a pool that is already closed."""


class PoolTimeoutError(RuntimeError):
    """No connection became available before the timeout elapsed."""


def _discard(conn: Connection) -> None:
    try:
        conn.close()
    except Exception:
        # the connection is being thrown away either way
        pass


class RedshiftConnectionPool:
    """A fixed-ceiling pool of ``redshift_connector`` connections.

    Args:
        connect_kwargs: The keywords every connection in this pool is opened
            with.
        max_size: The most connections that may exist at once.
        timeout: How long, in seconds, ``getconn()`` waits for one.
        configure: Called once with each newly-opened connection, before it is
            handed out, so a caller can apply session settings.
    """

    def __init__(
        self,
        connect_kwargs: dict[str, Any],
        max_size: int = 5,
        timeout: float = 30.0,
        configure: Callable[[Connection], None] | None = None,
    ) -> None:
        self.connect_kwargs = connect_kwargs
        self.max_size = max(1, max_size)
        self.timeout = timeout
        self.configure = configure
        self._lock = threading.Condition()
        self._idle: list[Connection] = []
        self._num_connections = 0
        self._closed = False

    def open_connection(self) -> Connection:
        """A new connection, opened and configured, outside of the pool's count.

        The main connection Harlequin runs queries on is not pooled -- it is
        held for the life of the session -- but it is opened and configured the
        same way as every pooled one.
        """
        conn: Connection = redshift_connector.connect(**self.connect_kwargs)
        try:
            if self.configure is not None:
                self.configure(conn)
        except Exception:
            _discard(conn)
            raise
        return conn

    def getconn(self, timeout: float | None = None) -> Connection:
        """Borrows a connection, opening one if the pool is below `max_size`.

        Raises:
            PoolClosedError: if the pool has been closed.
            PoolTimeoutError: if every connection is checked out for `timeout`.
        """
        deadline_wait = self.timeout if timeout is None else timeout
        with self._lock:
            while True:
                if self._closed:
                    raise PoolClosedError("This connection pool is closed.")
                if self._idle:
                    return self._idle.pop()
                if self._num_connections < self.max_size:
                    # reserve the slot before releasing the lock, so that
                    # concurrent borrowers cannot both open the last connection
                    self._num_connections += 1
                    break
                if not self._lock.wait(timeout=deadline_wait):
                    raise PoolTimeoutError(
                        f"Could not get a connection from the pool within "
                        f"{deadline_wait} seconds."
                    )
        try:
            return self.open_connection()
        except Exception:
            with self._lock:
                self._num_connections -= 1
                self._lock.notify()
            raise

    def putconn(self, conn: Connection, *, discard: bool = False) -> None:
        """Returns a borrowed connection to the pool.

        Args:
            conn: The connection to return.
            discard: True to close it and free its slot instead of reusing it,
                for a connection whose session state is no longer trustworthy.
        """
        with self._lock:
            if discard or self._closed:
                self._num_connections -= 1
                self._lock.notify()
            else:
                self._idle.append(conn)
                self._lock.notify()
                return
        _discard(conn)

    @contextmanager
    def connection(self, timeout: float | None = None) -> Iterator[Connection]:
        """A borrowed connection, returned to the pool when the block ends.

        A connection whose block raised is discarded rather than reused: the
        driver leaves the session's transaction state undefined after an error,
        and the next borrower must not inherit it.
        """
        conn = self.getconn(timeout=timeout)
        try:
            yield conn
        except BaseException:
            self.putconn(conn, discard=True)
            raise
        else:
            self.putconn(conn)

    def close(self) -> None:
        """Closes every idle connection and refuses further borrowing.

        A connection that is checked out right now is closed when it is
        returned.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            idle, self._idle = self._idle, []
            self._num_connections -= len(idle)
            self._lock.notify_all()
        for conn in idle:
            _discard(conn)
