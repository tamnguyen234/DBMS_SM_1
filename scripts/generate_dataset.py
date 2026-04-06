from __future__ import annotations

from argparse import ArgumentParser
from csv import writer
from pathlib import Path
from typing import Iterable


SEMESTERS = ["2023A", "2023B", "2024A", "2024B"]
DEPARTMENTS = ["CS", "IT", "SE", "IS", "AI", "DS", "BA", "CSIT"]
CLASS_NAMES = [f"K{year}{suffix}" for year in range(20, 36) for suffix in ("A", "B", "C")]
FIRST_NAMES = [
    "An",
    "Bình",
    "Chi",
    "Dũng",
    "Giang",
    "Hà",
    "Hạnh",
    "Hiếu",
    "Khánh",
    "Linh",
    "Minh",
    "Nam",
    "Nga",
    "Phương",
    "Quyên",
    "Sơn",
    "Tuấn",
    "Vy",
]
LAST_NAMES = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Phan", "Vũ", "Võ", "Đặng", "Bùi"]
COURSE_TITLES = [
    "Hệ cơ sở dữ liệu",
    "Cấu trúc dữ liệu",
    "Hệ điều hành",
    "Mạng máy tính",
    "Kỹ nghệ phần mềm",
    "Giải thuật",
    "Toán rời rạc",
    "Học máy",
    "Tìm kiếm thông tin",
    "Phát triển web",
]


def make_full_name(student_id: int) -> str:
    last_name = LAST_NAMES[student_id % len(LAST_NAMES)]
    middle_name = FIRST_NAMES[(student_id // len(LAST_NAMES)) % len(FIRST_NAMES)]
    given_name = FIRST_NAMES[(student_id // 7) % len(FIRST_NAMES)]
    return f"{last_name} {middle_name} {given_name}"


def make_student_row(student_id: int) -> list[str]:
    class_name = CLASS_NAMES[(student_id - 1) % len(CLASS_NAMES)]
    return [
        str(student_id),
        make_full_name(student_id),
        class_name,
        f"student{student_id:07d}@uni.edu",
        f"09{student_id % 1_000_000_00:08d}",
    ]


def make_course_row(course_id: int) -> list[str]:
    title = COURSE_TITLES[(course_id - 1) % len(COURSE_TITLES)]
    department = DEPARTMENTS[(course_id - 1) % len(DEPARTMENTS)]
    credits = 2 + ((course_id - 1) % 4)
    return [str(course_id), f"{title} {course_id:04d}", str(credits), department]


def make_enrollment_row(student_id: int, course_id: int, offset: int) -> list[str]:
    semester = SEMESTERS[(student_id + offset) % len(SEMESTERS)]
    score = 5.0 + ((student_id * 37 + course_id * 11 + offset * 13) % 51) / 10.0
    return [str(student_id), str(course_id), semester, f"{score:.1f}"]


def write_table(path: Path, header: list[str], rows: Iterable[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_handle:
        csv_writer = writer(file_handle)
        csv_writer.writerow(header)
        for row in rows:
            csv_writer.writerow(row)


def generate_dataset(
    output_dir: Path,
    student_count: int = 1_000_000,
    course_count: int = 500,
    enrollments_per_student: int = 1,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Đang tạo {student_count:,} sinh viên vào {output_dir / 'students.txt'}")
    write_table(
        output_dir / "students.txt",
        ["student_id", "full_name", "class_name", "email", "phone"],
        (make_student_row(student_id) for student_id in range(1, student_count + 1)),
    )

    print(f"Đang tạo {course_count:,} học phần vào {output_dir / 'courses.txt'}")
    write_table(
        output_dir / "courses.txt",
        ["course_id", "course_name", "credits", "dept_name"],
        (make_course_row(course_id) for course_id in range(1, course_count + 1)),
    )

    print(f"Đang tạo enrollment vào {output_dir / 'enrollments.txt'}")
    def enrollment_rows() -> Iterable[list[str]]:
        for student_id in range(1, student_count + 1):
            for offset in range(enrollments_per_student):
                course_id = ((student_id * 13) + offset * 7) % course_count + 1
                yield make_enrollment_row(student_id, course_id, offset)

    write_table(
        output_dir / "enrollments.txt",
        ["student_id", "course_id", "semester", "score"],
        enrollment_rows(),
    )

    print("Hoàn tất tạo dataset.")


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Sinh dataset quan hệ cho phần demo quản lý lưu trữ.")
    parser.add_argument("--output-dir", type=Path, default=Path("data"), help="Thư mục chứa các file TXT.")
    parser.add_argument("--student-count", type=int, default=1_000_000, help="Số lượng bản ghi sinh viên.")
    parser.add_argument("--course-count", type=int, default=500, help="Số lượng bản ghi học phần.")
    parser.add_argument(
        "--enrollments-per-student",
        type=int,
        default=1,
        help="Số dòng enrollment tạo cho mỗi sinh viên.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    generate_dataset(
        output_dir=args.output_dir,
        student_count=args.student_count,
        course_count=args.course_count,
        enrollments_per_student=args.enrollments_per_student,
    )


if __name__ == "__main__":
    main()
