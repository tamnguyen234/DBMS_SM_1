from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Enrollment:
    student_id: int
    course_id: int
    semester: str
    score: float

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Enrollment":
        return cls(
            student_id=int(row["student_id"]),
            course_id=int(row["course_id"]),
            semester=row["semester"],
            score=float(row["score"]),
        )

    def to_row(self) -> list[str]:
        return [
            str(self.student_id),
            str(self.course_id),
            self.semester,
            f"{self.score:.1f}",
        ]
