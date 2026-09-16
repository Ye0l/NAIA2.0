"""Small thread-safe LRU cache with both byte and entry limits."""
from collections import OrderedDict
from collections.abc import MutableMapping
from threading import RLock


class ByteBudgetCache(MutableMapping):
    def __init__(self, max_bytes, max_entries, sizeof):
        self.max_bytes = max_bytes
        self.max_entries = max_entries
        self.sizeof = sizeof
        self._items = OrderedDict()
        self._bytes = 0
        self._lock = RLock()

    @property
    def bytes_used(self):
        with self._lock:
            return self._bytes

    def __len__(self):
        with self._lock:
            return len(self._items)

    def __iter__(self):
        with self._lock:
            return iter(tuple(self._items))

    def __getitem__(self, key):
        with self._lock:
            value, _ = self._items[key]
            self._items.move_to_end(key)
            return value

    def __setitem__(self, key, value):
        size = max(0, int(self.sizeof(value)))
        with self._lock:
            if key in self._items:
                self.__delitem__(key)
            if size > self.max_bytes or self.max_entries <= 0:
                return
            while self._items and (
                self._bytes + size > self.max_bytes
                or len(self._items) >= self.max_entries
            ):
                _, (_, old_size) = self._items.popitem(last=False)
                self._bytes -= old_size
            self._items[key] = (value, size)
            self._bytes += size

    def __delitem__(self, key):
        with self._lock:
            _, size = self._items.pop(key)
            self._bytes -= size

    def clear(self):
        with self._lock:
            self._items.clear()
            self._bytes = 0
