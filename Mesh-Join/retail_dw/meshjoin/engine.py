"""MESHJOIN: the stream-relation join of PDF sections 4, 4.1 and 4.2.

Components, under the names the PDF uses:

    Stream buffer   stream_buffer.StreamBuffer  transactions waiting for an iteration
    Hash table H    hash_table.HashTable        the transactions in memory
    Queue           chunk_queue.ChunkQueue      the chunks of H, in order of arrival
    Disk buffer     disk_buffer.DiskBuffer      the one partition of R in memory
    Master Data R   disk_buffer.PartitionSource the tables that stay on disk

The project joins the stream S with two Master Data tables, so there is one Disk buffer
and one source per table, with Nc customer partitions and Np product partitions.

This module contains no database code and no clock. Master Data R is injected, so the same
join runs against MySQL in the program and against a list in the tests, and it behaves
deterministically for a given stream and given sources.

There is no dictionary of Master Data here, and no transaction ever searches R. Tuples
arrive only as whole partitions, and the probe direction is always a Master Data tuple
looked up in H. That is what lets one disk read serve every transaction in memory.
"""

from __future__ import annotations

from itertools import count

from retail_dw.config import CHUNK_SIZE_W
from retail_dw.meshjoin.chunk_queue import ChunkQueue
from retail_dw.meshjoin.disk_buffer import DiskBuffer, PartitionSource
from retail_dw.meshjoin.hash_table import HashTable
from retail_dw.meshjoin.model import Chunk
from retail_dw.meshjoin.stream_buffer import StreamBuffer


class MeshJoinEngine:
    """Runs the four steps of PDF 4.2, one iteration per call of run_iteration.

    Only MeshJoinThread may call run_iteration. An exception from a Master Data source
    leaves the cursors out of step with the visit counts, so it must end the run, and
    --resume then rebuilds this state from the database. The read-only properties may be
    called from any thread, and report single values that can be one iteration old.
    """

    def __init__(
        self,
        stream_buffer: StreamBuffer,
        customer_source: PartitionSource,
        product_source: PartitionSource,
        customer_key_index: int = 0,
        product_key_index: int = 0,
        w: int = CHUNK_SIZE_W,
    ) -> None:
        if w <= 0:
            raise ValueError("chunk size w must be greater than zero")
        self.stream_buffer = stream_buffer
        self.hash_table = HashTable()
        self.queue = ChunkQueue()
        self.customer_disk_buffer = DiskBuffer()
        self.product_disk_buffer = DiskBuffer()
        self.customer_master_data = customer_source
        self.product_master_data = product_source
        self._customer_key_index = customer_key_index
        self._product_key_index = product_key_index
        self._w = w
        self._chunk_ids = count(1)
        # Fixed for the run, because Master Data R does not change during one.
        self._nc = customer_source.partitions
        self._np = product_source.partitions

    def run_iteration(self) -> list[Chunk]:
        """One iteration of MESHJOIN. Returns the chunks that finished, oldest first.

        A transaction therefore appears in the result exactly once, when its chunk leaves
        H. Chunks leave in arrival order and a chunk keeps its internal order, so online
        rows come out in the order they were read from the file.
        """
        self._step1_read_next_partitions()
        self._step2_read_stream_chunk()
        self._step3_probe_h_with_master_data()
        return self._step4_remove_finished_chunks()

    def _step1_read_next_partitions(self) -> None:
        """Replace each Disk buffer with the next partition of its table. After the last
        partition, the source starts again at the first."""
        self.customer_disk_buffer.load(self.customer_master_data.next_partition())
        self.product_disk_buffer.load(self.product_master_data.next_partition())

    def _step2_read_stream_chunk(self) -> None:
        """Read up to w transactions from the Stream buffer into H as one chunk, and put
        its pointer on the Queue. A slow stream gives a partial chunk or no chunk at all,
        and the chunks already in H carry on scanning R either way."""
        transactions = self.stream_buffer.take(self._w)
        if transactions:
            chunk = Chunk(next(self._chunk_ids), transactions)
            self.hash_table.add_chunk(chunk)
            self.queue.append(chunk)

    def _step3_probe_h_with_master_data(self) -> None:
        """Probe the join attribute of each Master Data tuple in H, then count the visit.

        The customer pass adds the customer attributes and the product pass adds the
        product and the supplier. The store has no Master Data table in this project, so
        it is added when the row is consolidated, and the ReadMe records that reading.
        Quantity, price, time and sales mode are never touched.

        A chunk may see a partition of the shorter table twice, because it stays for
        max(Nc, Np) iterations. Setting the same tuple again changes nothing.
        """
        for customer_tuple in self.customer_disk_buffer:
            matches = self.hash_table.probe_customer(customer_tuple[self._customer_key_index])
            for transaction in matches:
                transaction.customer = customer_tuple
        for product_tuple in self.product_disk_buffer:
            matches = self.hash_table.probe_product(product_tuple[self._product_key_index])
            for transaction in matches:
                transaction.product = product_tuple
        for chunk in self.queue:
            chunk.customer_visits += 1
            chunk.product_visits += 1

    def _step4_remove_finished_chunks(self) -> list[Chunk]:
        """Remove a chunk only when it has been joined with the whole of both tables.

        Removing it one iteration sooner loses matches: its last partition could hold the
        tuple some row is waiting for. Only the oldest chunk can be finished, because every
        younger chunk has fewer visits, so chunks leave in the order they entered.
        """
        finished: list[Chunk] = []
        while (oldest := self.queue.oldest()) is not None and self._join_complete(oldest):
            self.queue.pop_oldest()
            self.hash_table.remove_chunk(oldest)
            finished.append(oldest)
        return finished

    def _join_complete(self, chunk: Chunk) -> bool:
        return chunk.customer_visits >= self._nc and chunk.product_visits >= self._np

    def has_work(self) -> bool:
        """True while a chunk is in H or a transaction waits in the Stream buffer."""
        return self.hash_table.size > 0 or self.stream_buffer.counts().total > 0

    @property
    def h_size(self) -> int:
        """Transactions in hash table H."""
        return self.hash_table.size

    @property
    def queue_len(self) -> int:
        """Chunks on the Queue."""
        return len(self.queue)

    @property
    def customer_position(self) -> int:
        """The customer partition read last, from 1 to Nc, or 0 before the first read."""
        return self.customer_master_data.position

    @property
    def product_position(self) -> int:
        """The product partition read last, from 1 to Np, or 0 before the first read."""
        return self.product_master_data.position

    @property
    def customer_partitions(self) -> int:
        """Nc: partitions in the customer table of Master Data R."""
        return self._nc

    @property
    def product_partitions(self) -> int:
        """Np: partitions in the product table of Master Data R."""
        return self._np

    @property
    def customer_disk_buffer_size(self) -> int:
        """Customer tuples in memory."""
        return len(self.customer_disk_buffer)

    @property
    def product_disk_buffer_size(self) -> int:
        """Product tuples in memory."""
        return len(self.product_disk_buffer)
