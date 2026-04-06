from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Generic, Iterable, TypeVar
import csv

T = TypeVar("T")


@dataclass(slots=True)
class Block(Generic[T]):
    """Một block vật lý có thể chứa tối đa `capacity` bản ghi."""

    block_id: int
    capacity: int
    records: list[T] = field(default_factory=list)

    def is_full(self) -> bool:
        return len(self.records) >= self.capacity

    def insert(self, record: T) -> None:
        if self.is_full():
            raise ValueError(f"Block {self.block_id} is full")
        self.records.append(record)

    def __len__(self) -> int:
        return len(self.records)


@dataclass(slots=True)
class QueryOutcome(Generic[T]):
    records: list[T]
    block_reads: int
    visited_blocks: list[int]


@dataclass(slots=True)
class BlockRange:
    low_key: Any
    high_key: Any


def read_records(path: Path, factory: Callable[[dict[str, str]], T]) -> list[T]:
    with path.open("r", encoding="utf-8", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        return [factory(row) for row in reader]


def build_blocks(records: Iterable[T], block_capacity: int) -> list[Block[T]]:
    blocks: list[Block[T]] = []
    current_block = Block(block_id=0, capacity=block_capacity)

    for record in records:
        if current_block.is_full():
            blocks.append(current_block)
            current_block = Block(block_id=len(blocks), capacity=block_capacity)
        current_block.insert(record)

    if current_block.records:
        blocks.append(current_block)

    for index, block in enumerate(blocks):
        block.block_id = index

    return blocks


def block_range(block: Block[T], key_fn: Callable[[T], Any]) -> BlockRange:
    if not block.records:
        return BlockRange(None, None)
    first_key = key_fn(block.records[0])
    last_key = key_fn(block.records[-1])
    return BlockRange(first_key, last_key)


def find_first_candidate_block(
    blocks: list[Block[T]],
    key: Any,
    key_fn: Callable[[T], Any],
) -> tuple[int | None, list[int]]:
    """Trả về block đầu tiên mà cận trên của khóa có thể chứa `key`."""

    visited_blocks: list[int] = []
    left = 0
    right = len(blocks) - 1
    candidate_index: int | None = None

    while left <= right:
        middle = (left + right) // 2
        visited_blocks.append(blocks[middle].block_id)
        current_range = block_range(blocks[middle], key_fn)
        if current_range.high_key is None:
            break
        if key <= current_range.high_key:
            candidate_index = middle
            right = middle - 1
        else:
            left = middle + 1

    return candidate_index, visited_blocks


def render_block_layout(
    blocks: list[Block[T]],
    record_formatter: Callable[[T], str] | None = None,
) -> str:
    formatter = record_formatter or (lambda record: str(record))
    lines: list[str] = []

    for block in blocks:
        if not block.records:
            lines.append(f"Khối {block.block_id}: rỗng")
            continue
        rendered_records = " | ".join(formatter(record) for record in block.records)
        lines.append(
            f"Khối {block.block_id} ({len(block.records)}/{block.capacity}): {rendered_records}"
        )

    return "\n".join(lines) if lines else "<không có block>"


def render_block_preview(
    blocks: list[Block[T]],
    record_formatter: Callable[[T], str] | None = None,
    preview_size: int = 3,
) -> str:
    if len(blocks) <= preview_size * 2:
        return render_block_layout(blocks, record_formatter)

    formatter = record_formatter or (lambda record: str(record))
    selected_blocks = blocks[:preview_size] + blocks[-preview_size:]
    lines = [
        *[
            f"Khối {block.block_id} ({len(block.records)}/{block.capacity}): "
            + " | ".join(formatter(record) for record in block.records)
            for block in selected_blocks[:preview_size]
        ],
        "...",
        *[
            f"Khối {block.block_id} ({len(block.records)}/{block.capacity}): "
            + " | ".join(formatter(record) for record in block.records)
            for block in selected_blocks[preview_size:]
        ],
    ]
    return "\n".join(lines)
