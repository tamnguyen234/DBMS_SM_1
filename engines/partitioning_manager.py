from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from models import Enrollment, Student

from .base import Block, QueryOutcome, build_blocks, read_records, render_block_layout


@dataclass(slots=True)
class PartitioningManager:
    """Phân vùng theo học kỳ cho file enrollment."""

    students: list[Student]
    enrollments: list[Enrollment]
    student_blocks: list[Block[Student]]
    semester_partitions: dict[str, list[Block[Enrollment]]]
    block_capacity: int

    @classmethod
    def from_data_files(
        cls,
        student_path: Path,
        enrollment_path: Path,
        block_capacity: int = 64,
    ) -> "PartitioningManager":
        students = read_records(student_path, Student.from_row)
        enrollments = read_records(enrollment_path, Enrollment.from_row)
        manager = cls(
            students=students,
            enrollments=enrollments,
            student_blocks=[],
            semester_partitions={},
            block_capacity=block_capacity,
        )
        manager._rebuild()
        return manager

    def _rebuild(self) -> None:
        self.student_blocks = build_blocks(self.students, self.block_capacity)
        partition_bucket: dict[str, list[Enrollment]] = {}
        for enrollment in self.enrollments:
            partition_bucket.setdefault(enrollment.semester, []).append(enrollment)

        self.semester_partitions = {}
        for semester, records in sorted(partition_bucket.items()):
            self.semester_partitions[semester] = build_blocks(records, self.block_capacity)

    def search_student(self, student_id: int) -> QueryOutcome[Student]:
        visited_blocks: list[int] = []
        matched: list[Student] = []
        for block in self.student_blocks:
            visited_blocks.append(block.block_id)
            for student in block.records:
                if student.student_id == student_id:
                    matched.append(student)
                    return QueryOutcome(matched, len(visited_blocks), visited_blocks)
        return QueryOutcome(matched, len(visited_blocks), visited_blocks)

    def list_enrollments(self, student_id: int, semester: str | None = None) -> QueryOutcome[Enrollment]:
        visited_blocks: list[int] = []
        matched: list[Enrollment] = []
        partitions = (
            {semester: self.semester_partitions.get(semester, [])}
            if semester is not None
            else self.semester_partitions
        )

        for partition_semester, blocks in partitions.items():
            for block in blocks:
                visited_blocks.append(block.block_id)
                for enrollment in block.records:
                    if enrollment.student_id != student_id:
                        continue
                    if semester is not None and partition_semester != semester:
                        continue
                    matched.append(enrollment)

        return QueryOutcome(matched, len(visited_blocks), visited_blocks)

    def insert_student(self, student: Student) -> int:
        self.students.append(student)
        self._rebuild()
        for block in self.student_blocks:
            if any(record.student_id == student.student_id for record in block.records):
                return block.block_id
        return -1

    def insert_enrollment(self, enrollment: Enrollment) -> int:
        self.enrollments.append(enrollment)
        self._rebuild()
        for block in self.semester_partitions.get(enrollment.semester, []):
            if any(
                record.student_id == enrollment.student_id and record.course_id == enrollment.course_id
                for record in block.records
            ):
                return block.block_id
        return -1

    def describe_student_blocks(self) -> str:
        return render_block_layout(self.student_blocks)

    def describe_enrollment_blocks(self) -> str:
        lines: list[str] = []
        for semester, blocks in self.semester_partitions.items():
            lines.append(f"Học kỳ {semester}")
            lines.append(render_block_layout(blocks))
        return "\n".join(lines) if lines else "<no partitions>"
