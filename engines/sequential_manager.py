from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from models import Enrollment, Student

from .base import Block, QueryOutcome, build_blocks, find_first_candidate_block, read_records, render_block_layout


@dataclass(slots=True)
class SequentialFileManager:
    """Tổ chức Sequential: bản ghi được lưu theo thứ tự khóa đã sắp xếp."""

    students: list[Student]
    enrollments: list[Enrollment]
    student_blocks: list[Block[Student]]
    enrollment_blocks: list[Block[Enrollment]]
    block_capacity: int

    @classmethod
    def from_data_files(
        cls,
        student_path: Path,
        enrollment_path: Path,
        block_capacity: int = 64,
    ) -> "SequentialFileManager":
        students = sorted(
            read_records(student_path, Student.from_row),
            key=lambda student: student.student_id,
        )
        enrollments = sorted(
            read_records(enrollment_path, Enrollment.from_row),
            key=lambda enrollment: (enrollment.student_id, enrollment.course_id, enrollment.semester),
        )
        return cls(
            students=students,
            enrollments=enrollments,
            student_blocks=build_blocks(students, block_capacity),
            enrollment_blocks=build_blocks(enrollments, block_capacity),
            block_capacity=block_capacity,
        )

    def _rebuild(self) -> None:
        self.students.sort(key=lambda student: student.student_id)
        self.enrollments.sort(key=lambda enrollment: (enrollment.student_id, enrollment.course_id, enrollment.semester))
        self.student_blocks = build_blocks(self.students, self.block_capacity)
        self.enrollment_blocks = build_blocks(self.enrollments, self.block_capacity)

    def search_student(self, student_id: int) -> QueryOutcome[Student]:
        candidate, visited_blocks = find_first_candidate_block(
            self.student_blocks,
            student_id,
            lambda student: student.student_id,
        )
        if candidate is None:
            return QueryOutcome([], len(visited_blocks), visited_blocks)

        block = self.student_blocks[candidate]
        visited_blocks.append(block.block_id)
        matched = [student for student in block.records if student.student_id == student_id]
        return QueryOutcome(matched, len(visited_blocks), visited_blocks)

    def list_enrollments(self, student_id: int, semester: str | None = None) -> QueryOutcome[Enrollment]:
        candidate, visited_blocks = find_first_candidate_block(
            self.enrollment_blocks,
            student_id,
            lambda enrollment: enrollment.student_id,
        )
        if candidate is None:
            return QueryOutcome([], len(visited_blocks), visited_blocks)

        matched: list[Enrollment] = []
        for index in range(candidate, len(self.enrollment_blocks)):
            block = self.enrollment_blocks[index]
            if not block.records:
                continue
            visited_blocks.append(block.block_id)
            first_student_id = block.records[0].student_id
            last_student_id = block.records[-1].student_id
            if first_student_id > student_id:
                break
            if last_student_id < student_id:
                continue
            for enrollment in block.records:
                if enrollment.student_id != student_id:
                    continue
                if semester is not None and enrollment.semester != semester:
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
        for block in self.enrollment_blocks:
            if any(record.student_id == enrollment.student_id and record.course_id == enrollment.course_id for record in block.records):
                return block.block_id
        return -1

    def describe_student_blocks(self) -> str:
        return render_block_layout(self.student_blocks)

    def describe_enrollment_blocks(self) -> str:
        return render_block_layout(self.enrollment_blocks)
