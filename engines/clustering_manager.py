from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from models import Enrollment, Student

from .base import Block, QueryOutcome, build_blocks, find_first_candidate_block, read_records, render_block_layout


@dataclass(slots=True)
class ClusterEntry:
    table: str
    payload: Student | Enrollment

    def key(self) -> int:
        return self.payload.student_id

    def label(self) -> str:
        prefix = "sinh viên" if self.table == "student" else "đăng ký học phần"
        return f"{prefix}: {self.payload}"


@dataclass(slots=True)
class MultitableClusteringManager:
    """Multitable clustering: các bản ghi liên quan từ nhiều bảng dùng chung cùng vùng vật lý."""

    students: list[Student]
    enrollments: list[Enrollment]
    cluster_blocks: list[Block[ClusterEntry]]
    block_capacity: int

    @classmethod
    def from_data_files(
        cls,
        student_path: Path,
        enrollment_path: Path,
        block_capacity: int = 64,
    ) -> "MultitableClusteringManager":
        students = read_records(student_path, Student.from_row)
        enrollments = read_records(enrollment_path, Enrollment.from_row)
        manager = cls(
            students=students,
            enrollments=enrollments,
            cluster_blocks=[],
            block_capacity=block_capacity,
        )
        manager._rebuild()
        return manager

    def _rebuild(self) -> None:
        clustered_entries: list[ClusterEntry] = []
        bucket: dict[int, list[ClusterEntry]] = {}

        for student in sorted(self.students, key=lambda item: item.student_id):
            bucket.setdefault(student.student_id, []).append(ClusterEntry("student", student))
        for enrollment in sorted(
            self.enrollments,
            key=lambda item: (item.student_id, item.course_id, item.semester),
        ):
            bucket.setdefault(enrollment.student_id, []).append(ClusterEntry("enrollment", enrollment))

        for student_id in sorted(bucket):
            clustered_entries.extend(bucket[student_id])

        self.cluster_blocks = build_blocks(clustered_entries, self.block_capacity)

    def search_student(self, student_id: int) -> QueryOutcome[Student]:
        candidate, visited_blocks = find_first_candidate_block(
            self.cluster_blocks,
            student_id,
            lambda entry: entry.key(),
        )
        if candidate is None:
            return QueryOutcome([], len(visited_blocks), visited_blocks)

        matched: list[Student] = []
        for index in range(candidate, len(self.cluster_blocks)):
            block = self.cluster_blocks[index]
            if not block.records:
                continue
            visited_blocks.append(block.block_id)
            first_key = block.records[0].key()
            last_key = block.records[-1].key()
            if first_key > student_id:
                break
            if last_key < student_id:
                continue
            for entry in block.records:
                if entry.key() != student_id or entry.table != "student":
                    continue
                matched.append(entry.payload)

        return QueryOutcome(matched, len(visited_blocks), visited_blocks)

    def list_enrollments(self, student_id: int, semester: str | None = None) -> QueryOutcome[Enrollment]:
        candidate, visited_blocks = find_first_candidate_block(
            self.cluster_blocks,
            student_id,
            lambda entry: entry.key(),
        )
        if candidate is None:
            return QueryOutcome([], len(visited_blocks), visited_blocks)

        matched: list[Enrollment] = []
        for index in range(candidate, len(self.cluster_blocks)):
            block = self.cluster_blocks[index]
            if not block.records:
                continue
            visited_blocks.append(block.block_id)
            first_key = block.records[0].key()
            last_key = block.records[-1].key()
            if first_key > student_id:
                break
            if last_key < student_id:
                continue
            for entry in block.records:
                if entry.key() != student_id or entry.table != "enrollment":
                    continue
                enrollment = entry.payload
                if semester is not None and enrollment.semester != semester:
                    continue
                matched.append(enrollment)

        return QueryOutcome(matched, len(visited_blocks), visited_blocks)

    def insert_student(self, student: Student) -> int:
        self.students.append(student)
        self._rebuild()
        for block in self.cluster_blocks:
            if any(entry.table == "student" and entry.payload.student_id == student.student_id for entry in block.records):
                return block.block_id
        return -1

    def insert_enrollment(self, enrollment: Enrollment) -> int:
        self.enrollments.append(enrollment)
        self._rebuild()
        for block in self.cluster_blocks:
            if any(
                entry.table == "enrollment"
                and entry.payload.student_id == enrollment.student_id
                and entry.payload.course_id == enrollment.course_id
                for entry in block.records
            ):
                return block.block_id
        return -1

    def describe_student_blocks(self) -> str:
        return render_block_layout(self.cluster_blocks, lambda entry: entry.label())

    def describe_enrollment_blocks(self) -> str:
        return render_block_layout(self.cluster_blocks, lambda entry: entry.label())
