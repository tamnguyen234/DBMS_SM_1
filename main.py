from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

from benchmarks.benchmark import benchmark_block_io, demo_insert_snapshot, format_benchmark_table, print_benchmark_rich


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Demo quản lý lưu trữ cho Heap, Sequential, Clustering và Partitioning.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="Thư mục chứa dataset TXT.")
    parser.add_argument("--block-capacity", type=int, default=64, help="Số bản ghi tối đa trong một block.")
    parser.add_argument("--student-id", type=int, default=1000, help="Mã sinh viên dùng trong truy vấn demo.")
    parser.add_argument(
        "--semester",
        type=str,
        default=None,
        help="Bộ lọc học kỳ tùy chọn khi liệt kê enrollment.",
    )
    parser.add_argument(
        "--skip-insert-demo",
        action="store_true",
        help="Bỏ qua phần chụp trạng thái block trước/sau khi chèn.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="Thư mục lưu kết quả chạy benchmark.",
    )
    return parser


def save_report(results_dir: Path, content: str) -> tuple[Path, Path]:
    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_file = results_dir / f"bao_cao_benchmark_{timestamp}.txt"
    latest_file = results_dir / "bao_cao_benchmark_moi_nhat.txt"
    run_file.write_text(content, encoding="utf-8")
    latest_file.write_text(content, encoding="utf-8")
    return run_file, latest_file


def main() -> None:
    args = build_parser().parse_args()
    student_txt = args.data_dir / "students.txt"
    course_txt = args.data_dir / "courses.txt"
    enrollment_txt = args.data_dir / "enrollments.txt"

    if not student_txt.exists() or not course_txt.exists() or not enrollment_txt.exists():
        print("Không tìm thấy dataset. Hãy chạy scripts/generate_dataset.py trước để tạo students.txt, courses.txt và enrollments.txt.")
        return

    rows = benchmark_block_io(
        data_dir=args.data_dir,
        student_id=args.student_id,
        semester=args.semester,
        block_capacity=args.block_capacity,
    )
    print_benchmark_rich(rows, title="So sánh I/O khối")
    report_sections: list[str] = [
        "THÔNG TIN CHẠY",
        f"- data_dir: {args.data_dir}",
        f"- student_id: {args.student_id}",
        f"- semester: {args.semester if args.semester is not None else 'không lọc'}",
        f"- block_capacity: {args.block_capacity}",
        "",
        format_benchmark_table(rows, title="So sánh I/O khối"),
    ]

    print(report_sections[-1])

    if not args.skip_insert_demo:
        snapshot = demo_insert_snapshot(args.data_dir, args.block_capacity)
        report_sections.extend(["", snapshot])
        print()
        print(snapshot)

    report_content = "\n".join(report_sections)
    run_file, latest_file = save_report(args.results_dir, report_content)
    print()
    print(f"Đã lưu báo cáo vào: {run_file}")
    print(f"Bản báo cáo mới nhất: {latest_file}")


if __name__ == "__main__":
    main()
