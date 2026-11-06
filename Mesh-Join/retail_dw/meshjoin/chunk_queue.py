"""Queue: the chunks in memory, in order of their arrival (PDF 4.1, Figure 3).

Each node of the Queue is a pointer to one chunk. The transactions themselves live in
hash table H, and the Queue never copies them. Its job is to answer one question for step
4 of an iteration: which chunk has been in memory the longest?

The PDF sizes the Queue by the partitions of the Master Data being scanned. With two tables,
that is the longer scan: step 4 keeps a chunk for max(Nc, Np) iterations and one chunk enters
per iteration, so the Queue never holds more than max(Nc, Np) chunks.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator

from retail_dw.meshjoin.model import Chunk


class ChunkQueue:
    """Queue of chunk references. As in the PDF, the oldest chunk is at the end."""

    def __init__(self) -> None:
        self._nodes: deque[Chunk] = deque()

    def append(self, chunk: Chunk) -> None:
        """Record the newest chunk, at the front of the Queue."""
        self._nodes.appendleft(chunk)

    def oldest(self) -> Chunk | None:
        """The chunk at the end of the Queue, or None when the Queue is empty."""
        return self._nodes[-1] if self._nodes else None

    def pop_oldest(self) -> Chunk:
        """Remove and return the chunk at the end. Raises IndexError when empty."""
        return self._nodes.pop()

    def __len__(self) -> int:
        return len(self._nodes)

    def __iter__(self) -> Iterator[Chunk]:
        """Walk from the newest chunk to the oldest."""
        return iter(self._nodes)
