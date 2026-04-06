from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from models import Enrollment, Student

from .base import Block, QueryOutcome, build_blocks, read_records, render_block_layout


@dataclass(slots=True)
class HeapFileManager:
    """Tổ chức Heap: chèn vào block đầu tiên còn chỗ trống."""

    student_blocks: list[Block[Student]]
    enrollment_blocks: list[Block[Enrollment]]
    block_capacity: int

    @classmethod
    def from_data_files(
        cls,
        student_path: Path,
        enrollment_path: Path,
        block_capacity: int = 64,
    ) -> "HeapFileManager":
        students = read_records(student_path, Student.from_row)
        enrollments = read_records(enrollment_path, Enrollment.from_row)
        return cls(
            student_blocks=build_blocks(students, block_capacity),
            enrollment_blocks=build_blocks(enrollments, block_capacity),
            block_capacity=block_capacity,
        )

    def search_student(self, student_id: int) -> QueryOutcome[Student]:
        visited_blocks: list[int] = []
        for block in self.student_blocks:
            visited_blocks.append(block.block_id)
            for student in block.records:
                if student.student_id == student_id:
                    return QueryOutcome([student], len(visited_blocks), visited_blocks)
        return QueryOutcome([], len(visited_blocks), visited_blocks)

    def list_enrollments(self, student_id: int, semester: str | None = None) -> QueryOutcome[Enrollment]:
        visited_blocks: list[int] = []
        matched: list[Enrollment] = []

        for block in self.enrollment_blocks:
            visited_blocks.append(block.block_id)
            for enrollment in block.records:
                if enrollment.student_id != student_id:
                    continue
                if semester is not None and enrollment.semester != semester:
                    continue
                matched.append(enrollment)

        return QueryOutcome(matched, len(visited_blocks), visited_blocks)

    def insert_student(self, student: Student) -> int:
        for block in self.student_blocks:
            if not block.is_full():
                block.insert(student)
                return block.block_id
        new_block = Block(block_id=len(self.student_blocks), capacity=self.block_capacity)
        new_block.insert(student)
        self.student_blocks.append(new_block)
        return new_block.block_id

    def insert_enrollment(self, enrollment: Enrollment) -> int:
        for block in self.enrollment_blocks:
            if not block.is_full():
                block.insert(enrollment)
                return block.block_id
        new_block = Block(block_id=len(self.enrollment_blocks), capacity=self.block_capacity)
        new_block.insert(enrollment)
        self.enrollment_blocks.append(new_block)
        return new_block.block_id

    def describe_student_blocks(self) -> str:
        return render_block_layout(self.student_blocks)

    def describe_enrollment_blocks(self) -> str:
        return render_block_layout(self.enrollment_blocks)
