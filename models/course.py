from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Course:
    course_id: int
    course_name: str
    credits: int
    dept_name: str

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Course":
        return cls(
            course_id=int(row["course_id"]),
            course_name=row["course_name"],
            credits=int(row["credits"]),
            dept_name=row["dept_name"],
        )

    def to_row(self) -> list[str]:
        return [
            str(self.course_id),
            self.course_name,
            str(self.credits),
            self.dept_name,
        ]
