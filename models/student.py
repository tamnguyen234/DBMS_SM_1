from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Student:
    student_id: int
    full_name: str
    class_name: str
    email: str
    phone: str

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "Student":
        return cls(
            student_id=int(row["student_id"]),
            full_name=row["full_name"],
            class_name=row["class_name"],
            email=row["email"],
            phone=row["phone"],
        )

    def to_row(self) -> list[str]:
        return [
            str(self.student_id),
            self.full_name,
            self.class_name,
            self.email,
            self.phone,
        ]
