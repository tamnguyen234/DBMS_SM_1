from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from rich.console import Console
from rich.table import Table

from engines import (
    HeapFileManager,
    MultitableClusteringManager,
    PartitioningManager,
    SequentialFileManager,
)
from engines.base import render_block_preview
from models import Student


@dataclass(slots=True)
class BenchmarkRow:
    method: str
    student_reads: int
    enrollment_reads: int
    total_reads: int
    elapsed_ms: float


def load_managers(data_dir: Path, block_capacity: int = 64) -> dict[str, object]:
    student_path = data_dir / "students.txt"
    enrollment_path = data_dir / "enrollments.txt"

    return {
        "Heap": HeapFileManager.from_data_files(student_path, enrollment_path, block_capacity),
        "Sequential": SequentialFileManager.from_data_files(student_path, enrollment_path, block_capacity),
        "Clustering": MultitableClusteringManager.from_data_files(student_path, enrollment_path, block_capacity),
        "Partitioning": PartitioningManager.from_data_files(student_path, enrollment_path, block_capacity),
    }


def benchmark_block_io(
    data_dir: Path,
    student_id: int,
    semester: str | None = None,
    block_capacity: int = 64,
) -> list[BenchmarkRow]:
    managers = load_managers(data_dir, block_capacity)
    rows: list[BenchmarkRow] = []

    for method_name, manager in managers.items():
        started_at = perf_counter()
        student_result = manager.search_student(student_id)
        enrollment_result = manager.list_enrollments(student_id, semester)
        elapsed_ms = (perf_counter() - started_at) * 1000
        rows.append(
            BenchmarkRow(
                method=method_name,
                student_reads=student_result.block_reads,
                enrollment_reads=enrollment_result.block_reads,
                total_reads=student_result.block_reads + enrollment_result.block_reads,
                elapsed_ms=elapsed_ms,
            )
        )

    return rows


def format_benchmark_table(rows: list[BenchmarkRow], title: str) -> str:
    headers = ["Phương pháp", "I/O sinh viên", "I/O enrollment", "Tổng I/O", "Thời gian (ms)"]
    columns = [
        [row.method for row in rows],
        [str(row.student_reads) for row in rows],
        [str(row.enrollment_reads) for row in rows],
        [str(row.total_reads) for row in rows],
        [f"{row.elapsed_ms:.2f}" for row in rows],
    ]
    widths = [len(header) for header in headers]
    for column_index, column in enumerate(columns):
        for cell in column:
            widths[column_index] = max(widths[column_index], len(cell))

    header_line = " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    separator = "-+-".join("-" * width for width in widths)
    body_lines = [
        " | ".join(column[row_index].ljust(widths[column_index]) for column_index, column in enumerate(columns))
        for row_index in range(len(rows))
    ]
    return "\n".join([title, header_line, separator, *body_lines])


def print_benchmark_rich(rows: list[BenchmarkRow], title: str) -> None:
    table = Table(title=title, show_lines=True)
    table.add_column("Phương pháp", style="bold")
    table.add_column("I/O sinh viên", justify="right")
    table.add_column("I/O enrollment", justify="right")
    table.add_column("Tổng I/O", justify="right")
    table.add_column("Thời gian (ms)", justify="right")

    for row in rows:
        table.add_row(
            row.method,
            str(row.student_reads),
            str(row.enrollment_reads),
            str(row.total_reads),
            f"{row.elapsed_ms:.2f}",
        )

    Console().print(table)


def demo_insert_snapshot(data_dir: Path, block_capacity: int = 64) -> str:
    heap_manager = HeapFileManager.from_data_files(
        data_dir / "students.txt",
        data_dir / "enrollments.txt",
        block_capacity,
    )
    before = ["Trước khi chèn:", render_block_preview(heap_manager.student_blocks)]
    new_student = Student(
        student_id=9_999_999,
        full_name="Sinh viên minh họa đã chèn",
        class_name="K99X",
        email="inserted.student@uni.edu",
        phone="0900999999",
    )
    heap_manager.insert_student(new_student)
    after = ["Sau khi chèn:", render_block_preview(heap_manager.student_blocks)]
    return "\n".join(before + [""] + after)
