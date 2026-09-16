"""Disk-backed Mapping for legacy JSON thumbnail packs.

Import one artist at a time; never materialize the multi-GB JSON document.
The original download is untouched. Only a completed SQLite cache is published.
Connections are short-lived so HTTP worker threads and Windows replacement do
not share SQLite handles. Values keep the legacy JSON format for compatibility.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile


def _fingerprint(path: Path) -> str:
    stat = path.stat()
    return json.dumps([str(path.resolve()), stat.st_size, stat.st_mtime_ns, 1])


class ArtistThumbnailStore(Mapping):
    def __init__(self, source: Path, cache_dir: Path):
        self.source = Path(source)
        cache_dir = Path(cache_dir).resolve()
        cache_dir.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(str(self.source.resolve()).encode()).hexdigest()[:24]
        self.path = cache_dir / f"{key}.sqlite3"
        self.fingerprint = _fingerprint(self.source)
        if not self._is_current():
            self._build()

    def _connect(self):
        connection = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
        connection.execute("PRAGMA cache_size=-2048")
        return connection

    def _is_current(self):
        if not self.path.is_file():
            return False
        try:
            with closing(self._connect()) as db:
                row = db.execute("SELECT value FROM metadata WHERE key='source'").fetchone()
                return row is not None and row[0] == self.fingerprint
        except sqlite3.DatabaseError:
            return False

    def _build(self):
        import ijson

        fd, name = tempfile.mkstemp(prefix=self.path.stem + "-", suffix=".tmp", dir=self.path.parent)
        os.close(fd)
        temp = Path(name)
        try:
            with closing(sqlite3.connect(temp)) as db:
                # This is an unpublished disposable file. The completed file is
                # atomically renamed only after commit and source validation.
                db.execute("PRAGMA journal_mode=OFF")
                db.execute("PRAGMA cache_size=-2048")
                db.execute("CREATE TABLE artists (name TEXT PRIMARY KEY, payload TEXT NOT NULL)")
                db.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                with self.source.open("rb") as stream:
                    first = stream.read(1)
                    while first and first.isspace():
                        first = stream.read(1)
                    if first != b"{":
                        raise ValueError("Artist thumbnail data is not a JSON object")
                    stream.seek(0)
                    for index, (artist, value) in enumerate(ijson.kvitems(stream, "", use_float=True)):
                        db.execute(
                            "INSERT INTO artists VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET payload=excluded.payload",
                            (artist, json.dumps(value, ensure_ascii=False)),
                        )
                        if index % 128 == 127:
                            db.commit()
                if _fingerprint(self.source) != self.fingerprint:
                    raise RuntimeError("Artist thumbnail source changed during import; retry")
                db.execute("INSERT INTO metadata VALUES ('source', ?)", (self.fingerprint,))
                db.commit()
            os.replace(temp, self.path)
        finally:
            temp.unlink(missing_ok=True)

    def __getitem__(self, key):
        with closing(self._connect()) as db:
            row = db.execute("SELECT payload FROM artists WHERE name=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def __iter__(self):
        with closing(self._connect()) as db:
            # Preserve source ordering (the legacy dict's tie-break order).
            keys = [row[0] for row in db.execute("SELECT name FROM artists ORDER BY rowid")]
        return iter(keys)

    def __len__(self):
        with closing(self._connect()) as db:
            return db.execute("SELECT COUNT(*) FROM artists").fetchone()[0]

    def __contains__(self, key):
        with closing(self._connect()) as db:
            return db.execute("SELECT 1 FROM artists WHERE name=?", (key,)).fetchone() is not None
