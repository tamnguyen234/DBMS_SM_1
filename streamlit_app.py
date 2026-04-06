from __future__ import annotations

import csv
import html
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import streamlit as st

from benchmarks.benchmark import benchmark_block_io, format_benchmark_table, load_managers
from engines.clustering_manager import ClusterEntry, MultitableClusteringManager
from engines.heap_manager import HeapFileManager
from engines.partitioning_manager import PartitioningManager
from engines.sequential_manager import SequentialFileManager
from engines.base import Block, QueryOutcome, build_blocks
from main import save_report
from models import Course, Enrollment, Student


st.set_page_config(page_title="Storage Management - Nhóm 10", page_icon="📦", layout="wide")


MANAGER_ORDER = ["Heap", "Sequential", "Clustering", "Partitioning"]

THEORY_TEXT = {
    "Heap": {
        "title": "Heap file",
        "problem": "Tối ưu ghi/chèn nhanh, chấp nhận đọc tuần tự khi cần tìm kiếm.",
        "works": "Bản ghi được thêm vào block đầu tiên còn chỗ trống. Khi truy vấn, thường phải quét nhiều block.",
        "when": "Dùng khi workload nghiêng về insert hoặc không có khóa tìm kiếm ổn định.",
        "pros": ["Chèn nhanh", "Cấu trúc đơn giản", "Ít chi phí duy trì thứ tự"],
        "cons": ["Tìm kiếm tốn I/O", "Dữ liệu không được sắp xếp"],
    },
    "Sequential": {
        "title": "Sequential file",
        "problem": "Tối ưu tra cứu theo khóa nhờ dữ liệu được sắp xếp.",
        "works": "Bản ghi được giữ theo thứ tự key, nên có thể bỏ qua nhiều block khi tìm kiếm.",
        "when": "Dùng khi truy vấn đọc/tìm kiếm nhiều hơn chèn.",
        "pros": ["Tìm kiếm tốt hơn heap", "Hợp với truy vấn theo dải khóa"],
        "cons": ["Insert phức tạp hơn", "Phải giữ thứ tự"],
    },
    "Clustering": {
        "title": "Multitable clustering",
        "problem": "Đưa các bản ghi liên quan ở nhiều bảng về gần nhau để giảm số block phải đọc.",
        "works": "Gom Student và Enrollment theo cùng student_id vào chung cụm block vật lý.",
        "when": "Dùng khi thường xuyên đọc các quan hệ liên kết theo cùng một khóa.",
        "pros": ["Giảm I/O khi join theo khóa", "Đọc dữ liệu liên quan gần nhau"],
        "cons": ["Cập nhật phức tạp", "Thiết kế phụ thuộc vào mẫu truy vấn"],
    },
    "Partitioning": {
        "title": "Partitioning",
        "problem": "Chia dữ liệu thành vùng nhỏ hơn để quét đúng phần cần thiết.",
        "works": "Enrollment được tách theo học kỳ, nên khi có lọc semester chỉ quét đúng phân vùng đó.",
        "when": "Dùng khi truy vấn thường lọc theo một thuộc tính phân vùng.",
        "pros": ["Giảm vùng quét", "Dễ tối ưu truy vấn theo điều kiện"],
        "cons": ["Phải chọn khóa phân vùng phù hợp", "Không tốt nếu truy vấn không bám partition key"],
    },
}

STATE_LABELS = {
    "normal": "Bình thường",
    "reading": "Đang đọc",
    "matched": "Khớp",
    "inserted": "Vừa chèn",
}


@dataclass(slots=True)
class DataPreview:
    rows: list[dict[str, str]]
    count: int


@dataclass(slots=True)
class DemoRecord:
    label: str
    kind: str = "student"


def page_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at 15% 10%, rgba(255, 193, 107, 0.22), transparent 32%),
                radial-gradient(circle at 80% 0%, rgba(124, 176, 221, 0.19), transparent 28%),
                linear-gradient(180deg, #f7f1e7 0%, #efe4d3 45%, #e7d8c4 100%);
            color: #241b12;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #1d2432 0%, #21293a 100%);
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }
        [data-testid="stSidebar"] * {
            color: #ecf1ff;
        }
        [data-testid="stSidebar"] .stNumberInput input,
        [data-testid="stSidebar"] .stTextInput input,
        [data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] {
            border-radius: 12px;
        }
        .hero {
            padding: 1.35rem 1.45rem 1.25rem 1.45rem;
            border: 1px solid rgba(61, 44, 29, 0.12);
            border-radius: 24px;
            background: linear-gradient(125deg, rgba(255, 252, 247, 0.93) 0%, rgba(255, 246, 232, 0.88) 100%);
            box-shadow: 0 24px 58px rgba(67, 43, 17, 0.10);
            margin-bottom: 1.1rem;
        }
        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.22em;
            font-size: 0.72rem;
            color: #86623b;
            font-weight: 800;
            margin-bottom: 0.45rem;
        }
        .hero-title {
            font-size: 2.25rem;
            line-height: 1.1;
            margin: 0;
            font-weight: 900;
            color: #1f1710;
        }
        .hero-subtitle {
            font-size: 1rem;
            color: #5d5145;
            margin-top: 0.55rem;
            max-width: 920px;
        }
        .chip-row {
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            margin-top: 0.9rem;
        }
        .chip {
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            background: #fff7ec;
            border: 1px solid rgba(134, 98, 59, 0.18);
            color: #5a432c;
            font-size: 0.82rem;
            font-weight: 700;
        }
        .panel {
            background: rgba(255, 252, 248, 0.95);
            border: 1px solid rgba(72, 50, 28, 0.10);
            border-radius: 20px;
            padding: 1rem 1rem 0.95rem 1rem;
            box-shadow: 0 14px 36px rgba(78, 54, 26, 0.07);
            margin-bottom: 1rem;
        }
        .section-title {
            font-size: 1.08rem;
            font-weight: 850;
            margin-bottom: 0.25rem;
            color: #22180f;
        }
        .section-subtitle {
            color: #66584b;
            font-size: 0.92rem;
            margin-bottom: 0.7rem;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 0.75rem;
            margin-bottom: 0.9rem;
        }
        .metric-card {
            border-radius: 16px;
            padding: 0.9rem 0.95rem;
            background: linear-gradient(180deg, #fffdf8 0%, #f8efe1 100%);
            border: 1px solid rgba(95, 67, 36, 0.10);
            box-shadow: 0 10px 24px rgba(80, 58, 31, 0.06);
        }
        .metric-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            color: #8f6a41;
            font-weight: 800;
        }
        .metric-value {
            font-size: 1.4rem;
            font-weight: 900;
            color: #1e1711;
            margin-top: 0.2rem;
        }
        .metric-help {
            color: #6a5b4c;
            font-size: 0.84rem;
            margin-top: 0.15rem;
        }
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 0.85rem;
        }
        .info-card {
            border-radius: 18px;
            padding: 1rem;
            border: 1px solid rgba(74, 53, 32, 0.10);
            background: rgba(255, 255, 255, 0.88);
        }
        .info-card h4 {
            margin: 0 0 0.5rem 0;
            font-size: 1rem;
            font-weight: 850;
            color: #1f1710;
        }
        .info-card p, .info-card li {
            color: #51463d;
            font-size: 0.92rem;
            line-height: 1.55;
        }
        .block-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(215px, 1fr));
            gap: 0.75rem;
            margin-top: 0.45rem;
        }
        .block-card {
            border-radius: 18px;
            padding: 0.9rem;
            border: 1px solid rgba(75, 55, 33, 0.14);
            background: linear-gradient(180deg, #fffefb 0%, #f7eee0 100%);
            box-shadow: 0 14px 28px rgba(80, 58, 31, 0.07);
            min-height: 155px;
        }
        .block-card.reading {
            border-color: rgba(122, 169, 109, 0.75);
            box-shadow: 0 0 0 1px rgba(122, 169, 109, 0.20) inset;
        }
        .block-card.matched {
            border-color: rgba(90, 124, 192, 0.75);
            box-shadow: 0 0 0 1px rgba(90, 124, 192, 0.20) inset;
        }
        .block-card.inserted {
            border-color: rgba(205, 112, 133, 0.75);
            box-shadow: 0 0 0 1px rgba(205, 112, 133, 0.20) inset;
        }
        .block-top {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 0.4rem;
            margin-bottom: 0.65rem;
        }
        .block-title {
            font-weight: 900;
            color: #1f1710;
            letter-spacing: 0.04em;
        }
        .block-pill {
            border-radius: 999px;
            padding: 0.23rem 0.58rem;
            font-size: 0.69rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #3b2c1b;
            background: #f1d9b4;
            white-space: nowrap;
        }
        .block-pill.reading { background: #d9ecbe; }
        .block-pill.matched { background: #d5e3fb; }
        .block-pill.inserted { background: #f4c1d0; }
        .record-pill {
            border-radius: 10px;
            padding: 0.4rem 0.5rem;
            margin-bottom: 0.32rem;
            font-size: 0.78rem;
            line-height: 1.3;
            font-weight: 700;
            color: #251a10;
            overflow-wrap: anywhere;
            background: #f9e7c1;
        }
        .record-pill.student { border-left: 4px solid #b06a38; }
        .record-pill.enrollment { border-left: 4px solid #3e79a3; }
        .record-pill.reading { background: #dfefb9; }
        .record-pill.matched { background: #d7e5fd; }
        .record-pill.inserted { background: #f5c3d2; }
        .small-note {
            color: #66584b;
            font-size: 0.88rem;
        }
        .tab-explain {
            margin-top: 0.2rem;
            padding: 0.9rem 1rem;
            border-left: 4px solid #b17d45;
            background: rgba(255, 249, 241, 0.92);
            border-radius: 0 14px 14px 0;
        }
        .stTabs [data-baseweb="tab"] {
            font-weight: 800;
            color: #3d2b16;
        }
        .stTabs [data-baseweb="tab"]:hover {
            color: #6d4019;
        }
        .stButton>button {
            border-radius: 12px;
            font-weight: 800;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def app_header() -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-kicker">Nhóm 10 · Storage Management 1</div>
            <h1 class="hero-title">Mô phỏng 4 cơ chế tổ chức file bằng block trực quan</h1>
            <div class="hero-subtitle">
                Chọn truy vấn ở panel bên phải, bấm chạy ở panel bên trái và theo dõi block được đọc theo thời gian thực.
                Bạn có thể chuyển nhanh giữa chế độ xem 2 block trọng tâm hoặc toàn bộ block.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def count_rows(file_path: str) -> int:
    path = Path(file_path)
    with path.open("r", encoding="utf-8", newline="") as file_handle:
        return max(sum(1 for _ in file_handle) - 1, 0)


@st.cache_data(show_spinner=False)
def preview_rows(file_path: str, limit: int = 5) -> list[dict[str, str]]:
    path = Path(file_path)
    with path.open("r", encoding="utf-8", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows


@st.cache_resource(show_spinner=False)
def get_cached_managers(data_dir: str, block_capacity: int) -> dict[str, Any]:
    return load_managers(Path(data_dir), block_capacity)


@st.cache_data(show_spinner=False)
def get_cached_benchmark_rows(data_dir: str, student_id: int, semester: str | None, block_capacity: int) -> list[Any]:
    return benchmark_block_io(Path(data_dir), student_id, semester, block_capacity)


def load_manager_for_kind(kind: str, data_dir: Path, block_capacity: int) -> Any:
    student_path = data_dir / "students.txt"
    enrollment_path = data_dir / "enrollments.txt"
    mapping: dict[str, Callable[[Path, Path, int], Any]] = {
        "Heap": HeapFileManager.from_data_files,
        "Sequential": SequentialFileManager.from_data_files,
        "Clustering": MultitableClusteringManager.from_data_files,
        "Partitioning": PartitioningManager.from_data_files,
    }
    return mapping[kind](student_path, enrollment_path, block_capacity)


def short_student(student: Student) -> str:
    return f"S{student.student_id} · {student.full_name} · {student.class_name}"


def short_enrollment(enrollment: Enrollment) -> str:
    return f"S{enrollment.student_id} / C{enrollment.course_id} · {enrollment.semester} · {enrollment.score:.1f}"


def short_course(course: Course) -> str:
    return f"C{course.course_id} · {course.course_name} · {course.dept_name}"


def short_cluster_entry(entry: ClusterEntry) -> str:
    if entry.table == "student":
        student = entry.payload
        return f"SV · {short_student(student)}"
    enrollment = entry.payload
    return f"DK · {short_enrollment(enrollment)}"


def block_renderer(record: Any) -> str:
    if isinstance(record, Student):
        return short_student(record)
    if isinstance(record, Enrollment):
        return short_enrollment(record)
    if isinstance(record, Course):
        return short_course(record)
    if isinstance(record, ClusterEntry):
        return short_cluster_entry(record)
    return str(record)


def get_block_html_class(block_id: int, visited_blocks: set[int], matched_blocks: set[int], inserted_block: int | None) -> tuple[str, str]:
    if inserted_block is not None and block_id == inserted_block:
        return "inserted", "Vừa chèn"
    if block_id in matched_blocks:
        return "matched", "Khớp"
    if block_id in visited_blocks:
        return "reading", "Đang đọc"
    return "normal", "Bình thường"


def select_block_indices(blocks: list[Block[Any]], focus_block_ids: set[int], head: int = 4, tail: int = 4) -> list[int]:
    if len(blocks) <= head + tail:
        return list(range(len(blocks)))
    selected = set(range(min(head, len(blocks))))
    selected.update(range(max(len(blocks) - tail, 0), len(blocks)))
    selected.update(block_id for block_id in focus_block_ids if 0 <= block_id < len(blocks))
    return sorted(selected)


def render_block_grid(
    title: str,
    blocks: list[Block[Any]],
    visited_blocks: set[int] | None = None,
    matched_blocks: set[int] | None = None,
    inserted_block: int | None = None,
    preview_head: int = 4,
    preview_tail: int = 4,
    record_formatter: Callable[[Any], str] = block_renderer,
    record_classifier: Callable[[Any], str] | None = None,
    wrap_panel: bool = True,
    force_indices: list[int] | None = None,
    note: str | None = None,
) -> None:
    visited_blocks = visited_blocks or set()
    matched_blocks = matched_blocks or set()
    selected_indices = force_indices or select_block_indices(
        blocks,
        visited_blocks | matched_blocks | ({inserted_block} if inserted_block is not None else set()),
        preview_head,
        preview_tail,
    )

    with st.container():
        outer_open = "<div class='panel'>" if wrap_panel else "<div>"
        outer_close = "</div>"
        st.markdown(f"{outer_open}<div class='section-title'>{html.escape(title)}</div>", unsafe_allow_html=True)
        if note:
            st.caption(note)
        if len(selected_indices) < len(blocks):
            st.caption(f"Chỉ hiển thị {len(selected_indices)} / {len(blocks)} block để tránh giao diện quá dài.")

        cards: list[str] = ["<div class='block-grid'>"]
        for index in selected_indices:
            block = blocks[index]
            state, badge = get_block_html_class(block.block_id, visited_blocks, matched_blocks, inserted_block)
            cards.append(f"<div class='block-card {state}'>")
            cards.append(
                "<div class='block-top'>"
                f"<div class='block-title'>BLOCK {block.block_id + 1}</div>"
                f"<div class='block-pill {state}'>{badge}</div>"
                "</div>"
            )
            cards.append(
                f"<div class='small-note'>Đầy {len(block.records)}/{block.capacity} bản ghi</div>"
            )
            if not block.records:
                cards.append("<div class='record-pill'>Rỗng</div>")
            else:
                for record in block.records:
                    record_class = record_classifier(record) if record_classifier else ""
                    extra_class = f" {record_class}" if record_class else ""
                    cards.append(
                        f"<div class='record-pill {state}{extra_class}'>{html.escape(record_formatter(record))}</div>"
                    )
            cards.append("</div>")
        cards.append(f"</div>{outer_close}")
        st.markdown("".join(cards), unsafe_allow_html=True)


def pick_focus_block_ids(blocks: list[Block[Any]], matched: set[int], limit: int = 2) -> list[int]:
    if not blocks:
        return []
    if matched:
        first_match = min(matched)
    else:
        first_match = 0
    focus = [first_match]
    next_block = first_match + 1
    if next_block < len(blocks) and len(focus) < limit:
        focus.append(next_block)
    return focus[:limit]


def stop_at_first_match(visited_blocks: list[int], matched_blocks: set[int]) -> set[int]:
    if not visited_blocks:
        return set()
    if not matched_blocks:
        return set(visited_blocks)
    cut_index = None
    for index, block_id in enumerate(visited_blocks):
        if block_id in matched_blocks:
            cut_index = index
            break
    if cut_index is None:
        return set(visited_blocks)
    return set(visited_blocks[: cut_index + 1])


def render_metric_row(items: list[tuple[str, str, str]]) -> None:
    st.markdown("<div class='metric-grid'>", unsafe_allow_html=True)
    for label, value, help_text in items:
        st.markdown(
            f"""
            <div class='metric-card'>
                <div class='metric-label'>{html.escape(label)}</div>
                <div class='metric-value'>{html.escape(value)}</div>
                <div class='metric-help'>{html.escape(help_text)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


def load_summary(data_dir: Path) -> dict[str, int]:
    return {
        "students": count_rows(str(data_dir / "students.txt")),
        "courses": count_rows(str(data_dir / "courses.txt")),
        "enrollments": count_rows(str(data_dir / "enrollments.txt")),
    }


def build_demo_blocks_heap() -> list[Block[DemoRecord]]:
    ids = [1, 2, 5, 3, 7, 4, 6, 8, 10, 9, 12, 11]
    records = [DemoRecord(f"S{student_id}") for student_id in ids]
    return build_blocks(records, 4)


def build_demo_blocks_sequential() -> list[Block[DemoRecord]]:
    records = [DemoRecord(f"S{student_id}") for student_id in range(1, 13)]
    return build_blocks(records, 4)


def build_demo_blocks_clustering() -> list[Block[DemoRecord]]:
    records: list[DemoRecord] = []
    for student_id in range(1, 7):
        records.append(DemoRecord(f"Student_{student_id}", "student"))
        records.append(DemoRecord(f"Enroll_{student_id}a", "enrollment"))
        records.append(DemoRecord(f"Enroll_{student_id}b", "enrollment"))
    return build_blocks(records, 6)


def build_demo_partitions() -> dict[str, list[Block[DemoRecord]]]:
    region_a = [DemoRecord(f"S{student_id}") for student_id in range(1, 9)]
    region_b = [DemoRecord(f"S{student_id}") for student_id in range(9, 17)]
    return {
        "Vùng A · ID 1-500": build_blocks(region_a, 4),
        "Vùng B · ID 501-1000": build_blocks(region_b, 4),
    }


def demo_record_label(record: Any) -> str:
    if isinstance(record, DemoRecord):
        return record.label
    return str(record)


def demo_record_kind(record: Any) -> str:
    if isinstance(record, DemoRecord):
        return record.kind
    if isinstance(record, ClusterEntry):
        return record.table
    return ""


def demo_record_id(record: Any) -> int | None:
    label = demo_record_label(record)
    if label.startswith("S"):
        suffix = label[1:]
    elif label.startswith("Student_"):
        suffix = label.split("_", maxsplit=1)[-1]
    else:
        return None
    digits = "".join(char for char in suffix if char.isdigit())
    return int(digits) if digits else None


def find_demo_matches(blocks: list[Block[Any]], student_id: int) -> set[int]:
    matched: set[int] = set()
    for block in blocks:
        for record in block.records:
            record_id = demo_record_id(record)
            if record_id == student_id:
                matched.add(block.block_id)
                break
    return matched


def simulate_binary_search(blocks: list[Block[Any]], student_id: int) -> set[int]:
    visited: set[int] = set()
    left = 0
    right = len(blocks) - 1
    while left <= right:
        mid = (left + right) // 2
        visited.add(mid)
        block = blocks[mid]
        ids = [demo_record_id(record) for record in block.records]
        ids = [value for value in ids if value is not None]
        if not ids:
            break
        if student_id < min(ids):
            right = mid - 1
        elif student_id > max(ids):
            left = mid + 1
        else:
            break
    return visited


def render_overview_tab(data_dir: Path, block_capacity: int, student_id: int, semester: str | None, results_dir: Path) -> None:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Tổng quan dữ liệu và demo</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-subtitle'>Tổng quan này giúp bạn chụp hình báo cáo nhanh: dữ liệu đầu vào, mục tiêu của từng cơ chế và nơi lưu kết quả.</div>",
        unsafe_allow_html=True,
    )
    summary = load_summary(data_dir)
    render_metric_row(
        [
            ("Students", f"{summary['students']:,}", "Bảng sinh viên dùng trong cả 4 cách tổ chức"),
            ("Courses", f"{summary['courses']:,}", "Bảng học phần phục vụ mô tả dataset"),
            ("Enrollments", f"{summary['enrollments']:,}", "Bảng đăng ký dùng cho truy vấn theo student/semester"),
            ("Block capacity", str(block_capacity), "Số bản ghi tối đa trong một block mô phỏng"),
        ]
    )
    st.markdown("</div>", unsafe_allow_html=True)

    left, right = st.columns([1.05, 0.95], gap="large")
    with left:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Dữ liệu đầu vào</div>", unsafe_allow_html=True)
        st.markdown("<div class='section-subtitle'>Mẫu 5 dòng đầu của từng file TXT để bạn minh hoạ dataset trong báo cáo.</div>", unsafe_allow_html=True)
        for title, file_name, formatter in [
            ("Student", "students.txt", short_student),
            ("Course", "courses.txt", short_course),
            ("Enrollment", "enrollments.txt", short_enrollment),
        ]:
            preview = preview_rows(str(data_dir / file_name), 5)
            with st.expander(f"Xem mẫu {title}", expanded=title == "Student"):
                if title == "Student":
                    st.write([Student.from_row(row) for row in preview])
                elif title == "Course":
                    st.write([Course.from_row(row) for row in preview])
                else:
                    st.write([Enrollment.from_row(row) for row in preview])
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Bám sát đề bài</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='tab-explain'>"
            "<p><strong>Nhóm 10</strong> có thể dùng giao diện này để demo theo đúng yêu cầu trong ảnh:</p>"
            "<ul>"
            "<li>Trực quan block trước/sau khi chèn.</li>"
            "<li>Chạy truy vấn giả lập và đếm số block phải đọc.</li>"
            "<li>So sánh Heap, Sequential, Clustering và Partitioning trên cùng một dataset.</li>"
            "<li>Chỉ ra rõ vì sao clustering khác partitioning.</li>"
            "</ul>"
            "</div>",
            unsafe_allow_html=True,
        )
        st.caption(f"Truy vấn đang dùng mặc định: student_id = {student_id}, semester = {semester if semester else 'không lọc'}")
        st.caption(f"Báo cáo được lưu trong {results_dir}")
        st.markdown("</div>", unsafe_allow_html=True)


def render_query_panel(student_id: int, semester: str | None) -> tuple[int, str | None]:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Bảng điều khiển truy vấn</div>", unsafe_allow_html=True)
    tab_query, tab_hint = st.tabs(["Chạy truy vấn", "Gợi ý"])
    with tab_query:
        query_student_id = st.number_input(
            "Student ID",
            min_value=1,
            value=int(student_id),
            step=1,
            key="demo_panel_student_id",
        )
        query_semester = st.selectbox(
            "Học kỳ lọc enrollment",
            options=["", "2023A", "2023B", "2024A", "2024B"],
            format_func=lambda x: "(Không lọc)" if x == "" else x,
            index=3 if semester else 0,
            key="demo_panel_semester",
        )
    with tab_hint:
        st.caption("Heap quét toàn bộ, Sequential nhảy theo binary search, Clustering gom dữ liệu liên quan, Partitioning chỉ quét đúng vùng.")
        st.caption("Giữ nguyên khối chưa đọc để thấy vùng nào không bị truy cập.")
    st.markdown("</div>", unsafe_allow_html=True)
    return int(query_student_id), (query_semester or None)


def render_demo_grid(
    managers: dict[str, Any],
    student_id: int,
    semester: str | None,
    display_mode: str,
) -> None:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Mô phỏng trực quan 4 cách tổ chức file</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-subtitle'>Block đổi màu theo dữ liệu thật đã được truy xuất khi chạy truy vấn.</div>",
        unsafe_allow_html=True,
    )
    run_query = st.button("Chạy truy vấn", type="primary", key="left_run_query")
    st.markdown("</div>", unsafe_allow_html=True)

    heap_manager: HeapFileManager = managers["Heap"]
    seq_manager: SequentialFileManager = managers["Sequential"]
    cluster_manager: MultitableClusteringManager = managers["Clustering"]
    partition_manager: PartitioningManager = managers["Partitioning"]

    heap_blocks = heap_manager.student_blocks
    seq_blocks = seq_manager.student_blocks
    cluster_blocks = cluster_manager.cluster_blocks
    partition_blocks = partition_manager.semester_partitions

    heap_visited: set[int] = set()
    seq_visited: set[int] = set()
    cluster_visited: set[int] = set()
    heap_matched: set[int] = set()
    seq_matched: set[int] = set()
    cluster_matched: set[int] = set()
    partition_visited: dict[str, set[int]] = {key: set() for key in partition_blocks}
    partition_matched: dict[str, set[int]] = {key: set() for key in partition_blocks}
    focus_ids: dict[str, list[int]] = {
        "heap": [],
        "seq": [],
        "cluster": [],
    }
    partition_focus_ids: dict[str, list[int]] = {key: [] for key in partition_blocks}

    if run_query:
        st.session_state["demo_has_run"] = True
        st.session_state["demo_last_student_id"] = int(student_id)
        st.session_state["demo_last_semester"] = semester or ""

    should_run = st.session_state.get("demo_has_run", False)
    active_student_id = int(st.session_state.get("demo_last_student_id", student_id))
    active_semester = st.session_state.get("demo_last_semester", semester or "") or None

    if should_run:
        heap_result: QueryOutcome[Student] = heap_manager.search_student(active_student_id)
        seq_result: QueryOutcome[Student] = seq_manager.search_student(active_student_id)
        cluster_result: QueryOutcome[Student] = cluster_manager.search_student(active_student_id)
        heap_matched = find_matching_blocks(
            heap_blocks, lambda record: isinstance(record, Student) and record.student_id == active_student_id
        )
        seq_matched = find_matching_blocks(
            seq_blocks, lambda record: isinstance(record, Student) and record.student_id == active_student_id
        )
        cluster_matched = find_matching_blocks(
            cluster_blocks,
            lambda record: isinstance(record, ClusterEntry)
            and ((record.table == "student" and record.payload.student_id == active_student_id)
                 or (record.table == "enrollment" and record.payload.student_id == active_student_id)),
        )
        heap_visited = stop_at_first_match(heap_result.visited_blocks, heap_matched)
        seq_visited = stop_at_first_match(seq_result.visited_blocks, seq_matched)
        if seq_matched:
            first_seq_match = min(seq_matched)
            seq_visited = {block_id for block_id in seq_visited if block_id <= first_seq_match}
        cluster_visited = stop_at_first_match(cluster_result.visited_blocks, cluster_matched)
        if active_semester:
            partition_result: QueryOutcome[Enrollment] = partition_manager.list_enrollments(active_student_id, active_semester)
            if active_semester in partition_blocks:
                partition_matched[active_semester] = find_matching_blocks(
                    partition_blocks[active_semester],
                    lambda record: isinstance(record, Enrollment) and record.student_id == active_student_id,
                )
                partition_visited[active_semester] = stop_at_first_match(
                    partition_result.visited_blocks,
                    partition_matched[active_semester],
                )

    if display_mode == "focus":
        focus_ids["heap"] = pick_focus_block_ids(heap_blocks, heap_matched, limit=2)
        focus_ids["seq"] = pick_focus_block_ids(seq_blocks, seq_matched, limit=2)
        focus_ids["cluster"] = pick_focus_block_ids(cluster_blocks, cluster_matched, limit=2)
        for semester_name, blocks in partition_blocks.items():
            partition_focus_ids[semester_name] = pick_focus_block_ids(
                blocks,
                partition_matched.get(semester_name, set()),
                limit=2,
            )

    row_one = st.columns(2, gap="large")
    with row_one[0]:
        render_block_grid(
            "Heap File (Đổ đống)",
            heap_blocks,
            visited_blocks=heap_visited,
            matched_blocks=heap_matched,
            force_indices=focus_ids["heap"] if display_mode == "focus" else None,
            note="Bản ghi đổ lộn xộn; khi tìm kiếm phải quét tuần tự.",
        )
    with row_one[1]:
        render_block_grid(
            "Sequential File (Tuần tự)",
            seq_blocks,
            visited_blocks=seq_visited,
            matched_blocks=seq_matched,
            force_indices=focus_ids["seq"] if display_mode == "focus" else None,
            note="Bản ghi sắp xếp theo student_id; truy vấn nhảy theo vùng khóa.",
        )

    row_two = st.columns(2, gap="large")
    with row_two[0]:
        render_block_grid(
            "Multitable Clustering (Gom cụm)",
            cluster_blocks,
            visited_blocks=cluster_visited,
            matched_blocks=cluster_matched,
            record_classifier=demo_record_kind,
            force_indices=focus_ids["cluster"] if display_mode == "focus" else None,
            note="Một block chứa xen kẽ Student và Enrollment cùng ID.",
        )
    with row_two[1]:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Partitioning (Phân mảnh)</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='section-subtitle'>Chỉ phân vùng đúng học kỳ bị quét; phân vùng còn lại giữ nguyên.</div>",
            unsafe_allow_html=True,
        )
        partition_items = list(partition_blocks.items())
        if active_semester and active_semester in partition_blocks:
            partition_items = [(active_semester, partition_blocks[active_semester])] + [
                item for item in partition_items if item[0] != active_semester
            ]
        partition_items = partition_items[:2]
        for semester_name, blocks in partition_items:
            render_block_grid(
                f"Học kỳ {html.escape(semester_name)}",
                blocks,
                visited_blocks=partition_visited.get(semester_name, set()),
                matched_blocks=partition_matched.get(semester_name, set()),
                force_indices=partition_focus_ids.get(semester_name) if display_mode == "focus" else None,
                wrap_panel=False,
            )
        st.markdown("</div>", unsafe_allow_html=True)


def render_comparison_table(data_dir: Path, block_capacity: int, student_id: int, semester: str | None) -> None:
    rows = get_cached_benchmark_rows(str(data_dir), int(student_id), semester, block_capacity)
    totals = [row.total_reads for row in rows]
    best_total = min(totals) if totals else 1
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Comparison Table</div>", unsafe_allow_html=True)
    st.dataframe(
        [
            {
                "Kiểu tổ chức file": row.method,
                "Blocks Accessed": row.total_reads,
                "Tổng thời gian (ms)": round(row.elapsed_ms, 2),
                "Hiệu quả (%)": round((best_total / row.total_reads) * 100, 1) if row.total_reads else 0,
            }
            for row in rows
        ],
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Hiệu quả được chuẩn hoá theo số block thấp nhất (100% là tốt nhất).")
    st.markdown("</div>", unsafe_allow_html=True)


def render_theory_card(kind: str) -> None:
    theory = THEORY_TEXT[kind]
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-title'>{html.escape(theory['title'])}</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='section-subtitle'>{html.escape(theory['problem'])}</div>",
        unsafe_allow_html=True,
    )
    cols = st.columns(3)
    with cols[0]:
        st.markdown("<div class='info-card'><h4>Nó hoạt động như thế nào?</h4><p>" + html.escape(theory["works"]) + "</p></div>", unsafe_allow_html=True)
    with cols[1]:
        st.markdown(
            "<div class='info-card'><h4>Ưu điểm</h4><ul>"
            + "".join(f"<li>{html.escape(item)}</li>" for item in theory["pros"])
            + "</ul></div>",
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            "<div class='info-card'><h4>Nhược điểm / khi dùng</h4><ul>"
            + "".join(f"<li>{html.escape(item)}</li>" for item in theory["cons"])
            + f"<li>{html.escape(theory['when'])}</li>"
            + "</ul></div>",
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


def find_matching_blocks(blocks: list[Block[Any]], predicate: Callable[[Any], bool]) -> set[int]:
    matched: set[int] = set()
    for block in blocks:
        if any(predicate(record) for record in block.records):
            matched.add(block.block_id)
    return matched


def show_search_result(kind: str, manager: Any, student_id: int, semester: str | None) -> None:
    student_result: QueryOutcome[Student] = manager.search_student(student_id)
    enrollment_result: QueryOutcome[Enrollment] = manager.list_enrollments(student_id, semester)
    st.success(
        f"Đã đọc {student_result.block_reads + enrollment_result.block_reads} block, tìm thấy {len(student_result.records)} sinh viên và {len(enrollment_result.records)} enrollment."
    )
    st.caption(f"Block đã đi qua: {student_result.visited_blocks + enrollment_result.visited_blocks}")
    if student_result.records:
        st.table([student.__dict__ for student in student_result.records])
    if enrollment_result.records:
        st.table([enrollment.__dict__ for enrollment in enrollment_result.records[:20]])


def render_heap_tab(manager: HeapFileManager, data_dir: Path, block_capacity: int) -> None:
    render_theory_card("Heap")
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Demo thao tác Heap</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-subtitle'>Chèn nhanh vào block còn trống, còn tra cứu thì phải quét tuần tự.</div>", unsafe_allow_html=True)
    left, right = st.columns(2, gap="large")
    with left:
        with st.form("heap_search_form"):
            search_student = st.number_input("Mã sinh viên để tìm", min_value=1, value=5, step=1, key="heap_search_student")
            search_semester = st.selectbox(
                "Học kỳ lọc enrollment",
                options=["", "2023A", "2023B", "2024A", "2024B"],
                format_func=lambda x: "(Không lọc)" if x == "" else x,
                index=0,
                key="heap_search_semester",
            )
            submitted = st.form_submit_button("Chạy tìm kiếm")
        if submitted:
            semester_filter = search_semester.strip() or None
            show_search_result("Heap", manager, int(search_student), semester_filter)
    with right:
        with st.form("heap_insert_form"):
            student_id = st.number_input("student_id", min_value=1, value=9_999_999, step=1, key="heap_insert_student_id")
            full_name = st.text_input("full_name", value="Sinh viên minh họa đã chèn", key="heap_insert_full_name")
            class_name = st.text_input("class_name", value="K99X", key="heap_insert_class_name")
            email = st.text_input("email", value="inserted.student@uni.edu", key="heap_insert_email")
            phone = st.text_input("phone", value="0900999999", key="heap_insert_phone")
            submitted = st.form_submit_button("Xem trước trước/sau chèn")
        if submitted:
            fresh_manager = load_manager_for_kind("Heap", data_dir, block_capacity)
            before_blocks = fresh_manager.student_blocks
            inserted_block = fresh_manager.insert_student(Student(int(student_id), full_name, class_name, email, phone))
            after_blocks = fresh_manager.student_blocks
            st.caption(f"Sinh viên sẽ được chèn vào block {inserted_block + 1}.")
            st.markdown("#### Trước khi chèn")
            render_block_grid("Heap - sinh viên", before_blocks, preview_head=3, preview_tail=3)
            st.markdown("#### Sau khi chèn")
            render_block_grid("Heap - sinh viên", after_blocks, inserted_block=inserted_block, preview_head=3, preview_tail=3)
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Bố cục block hiện tại</div>", unsafe_allow_html=True)
    render_block_grid("Heap - khối sinh viên", manager.student_blocks, preview_head=4, preview_tail=4)
    render_block_grid("Heap - khối enrollment", manager.enrollment_blocks, preview_head=4, preview_tail=4)
    st.markdown("</div>", unsafe_allow_html=True)


def render_sequential_tab(manager: SequentialFileManager, data_dir: Path, block_capacity: int) -> None:
    render_theory_card("Sequential")
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Demo thao tác Sequential</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-subtitle'>Dữ liệu được giữ theo thứ tự khóa, nên tìm kiếm nhảy vào vùng phù hợp thay vì quét toàn bộ.</div>", unsafe_allow_html=True)
    left, right = st.columns(2, gap="large")
    with left:
        with st.form("seq_search_form"):
            search_student = st.number_input("Mã sinh viên để tìm", min_value=1, value=5, step=1, key="seq_search_student")
            search_semester = st.selectbox(
                "Học kỳ lọc enrollment",
                options=["", "2023A", "2023B", "2024A", "2024B"],
                format_func=lambda x: "(Không lọc)" if x == "" else x,
                index=0,
                key="seq_search_semester",
            )
            submitted = st.form_submit_button("Chạy tìm kiếm")
        if submitted:
            semester_filter = search_semester.strip() or None
            show_search_result("Sequential", manager, int(search_student), semester_filter)
    with right:
        with st.form("seq_insert_form"):
            student_id = st.number_input("student_id", min_value=1, value=9_999_999, step=1, key="seq_insert_student_id")
            full_name = st.text_input("full_name", value="Sinh viên minh họa đã chèn", key="seq_insert_full_name")
            class_name = st.text_input("class_name", value="K99X", key="seq_insert_class_name")
            email = st.text_input("email", value="inserted.student@uni.edu", key="seq_insert_email")
            phone = st.text_input("phone", value="0900999999", key="seq_insert_phone")
            submitted = st.form_submit_button("Xem trước trước/sau chèn")
        if submitted:
            fresh_manager = load_manager_for_kind("Sequential", data_dir, block_capacity)
            before_blocks = fresh_manager.student_blocks
            inserted_block = fresh_manager.insert_student(Student(int(student_id), full_name, class_name, email, phone))
            after_blocks = fresh_manager.student_blocks
            st.caption(f"Sau khi chèn, cấu trúc phải rebuild; record mới nằm ở block {inserted_block + 1}.")
            st.markdown("#### Trước khi chèn")
            render_block_grid("Sequential - sinh viên", before_blocks, preview_head=3, preview_tail=3)
            st.markdown("#### Sau khi chèn")
            render_block_grid("Sequential - sinh viên", after_blocks, inserted_block=inserted_block, preview_head=3, preview_tail=3)
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Bố cục block hiện tại</div>", unsafe_allow_html=True)
    render_block_grid("Sequential - khối sinh viên", manager.student_blocks, preview_head=4, preview_tail=4)
    render_block_grid("Sequential - khối enrollment", manager.enrollment_blocks, preview_head=4, preview_tail=4)
    st.markdown("</div>", unsafe_allow_html=True)


def render_clustering_tab(manager: MultitableClusteringManager, data_dir: Path, block_capacity: int) -> None:
    render_theory_card("Clustering")
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Demo thao tác Multitable clustering</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-subtitle'>Khác với partitioning, clustering gom Student và Enrollment liên quan theo student_id vào cùng vùng vật lý để giảm I/O khi đọc dữ liệu liên kết.</div>",
        unsafe_allow_html=True,
    )
    left, right = st.columns(2, gap="large")
    with left:
        with st.form("cluster_search_form"):
            search_student = st.number_input("Mã sinh viên để tìm", min_value=1, value=5, step=1, key="cluster_search_student")
            search_semester = st.selectbox(
                "Học kỳ lọc enrollment",
                options=["", "2023A", "2023B", "2024A", "2024B"],
                format_func=lambda x: "(Không lọc)" if x == "" else x,
                index=0,
                key="cluster_search_semester",
            )
            submitted = st.form_submit_button("Chạy tìm kiếm")
        if submitted:
            semester_filter = search_semester.strip() or None
            show_search_result("Clustering", manager, int(search_student), semester_filter)
    with right:
        with st.form("cluster_insert_form"):
            student_id = st.number_input("student_id", min_value=1, value=9_999_999, step=1, key="cluster_insert_student_id")
            full_name = st.text_input("full_name", value="Sinh viên minh họa đã chèn", key="cluster_insert_full_name")
            class_name = st.text_input("class_name", value="K99X", key="cluster_insert_class_name")
            email = st.text_input("email", value="inserted.student@uni.edu", key="cluster_insert_email")
            phone = st.text_input("phone", value="0900999999", key="cluster_insert_phone")
            submitted = st.form_submit_button("Xem trước trước/sau chèn")
        if submitted:
            fresh_manager = load_manager_for_kind("Clustering", data_dir, block_capacity)
            before_blocks = fresh_manager.cluster_blocks
            inserted_block = fresh_manager.insert_student(Student(int(student_id), full_name, class_name, email, phone))
            after_blocks = fresh_manager.cluster_blocks
            st.caption(f"Record mới được đặt vào cụm của student_id = {int(student_id)} (block {inserted_block + 1}).")
            st.markdown("#### Trước khi chèn")
            render_block_grid("Clustering - khối chung", before_blocks, preview_head=3, preview_tail=3)
            st.markdown("#### Sau khi chèn")
            render_block_grid("Clustering - khối chung", after_blocks, inserted_block=inserted_block, preview_head=3, preview_tail=3)
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Bố cục block hiện tại</div>", unsafe_allow_html=True)
    render_block_grid("Clustering - các cụm block", manager.cluster_blocks, preview_head=4, preview_tail=4)
    st.markdown("</div>", unsafe_allow_html=True)


def render_partitioning_tab(manager: PartitioningManager, data_dir: Path, block_capacity: int) -> None:
    render_theory_card("Partitioning")
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Demo thao tác Partitioning</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-subtitle'>Partitioning chia enrollment theo semester, nên truy vấn có lọc học kỳ chỉ quét đúng phân vùng đó.</div>",
        unsafe_allow_html=True,
    )
    left, right = st.columns(2, gap="large")
    with left:
        with st.form("part_search_form"):
            search_student = st.number_input("Mã sinh viên để tìm", min_value=1, value=5, step=1, key="part_search_student")
            search_semester = st.selectbox(
                "Học kỳ lọc enrollment",
                options=["", "2023A", "2023B", "2024A", "2024B"],
                format_func=lambda x: "(Không lọc)" if x == "" else x,
                index=3,
                key="part_search_semester",
            )
            submitted = st.form_submit_button("Chạy tìm kiếm")
        if submitted:
            semester_filter = search_semester.strip() or None
            show_search_result("Partitioning", manager, int(search_student), semester_filter)
    with right:
        with st.form("part_insert_form"):
            student_id = st.number_input("student_id", min_value=1, value=9_999_999, step=1, key="part_insert_student_id")
            full_name = st.text_input("full_name", value="Sinh viên minh họa đã chèn", key="part_insert_full_name")
            class_name = st.text_input("class_name", value="K99X", key="part_insert_class_name")
            email = st.text_input("email", value="inserted.student@uni.edu", key="part_insert_email")
            phone = st.text_input("phone", value="0900999999", key="part_insert_phone")
            semester = st.selectbox(
                "semester",
                options=["2023A", "2023B", "2024A", "2024B"],
                index=2,
                key="part_insert_semester",
            )
            course_id = st.number_input("course_id", min_value=1, value=999, step=1, key="part_insert_course_id")
            score = st.number_input("score", min_value=0.0, max_value=10.0, value=8.5, step=0.1, key="part_insert_score")
            submitted = st.form_submit_button("Xem trước trước/sau chèn")
        if submitted:
            fresh_manager = load_manager_for_kind("Partitioning", data_dir, block_capacity)
            before_student_blocks = fresh_manager.student_blocks
            before_partition_blocks = fresh_manager.semester_partitions.get(semester, [])
            inserted_student_block = fresh_manager.insert_student(Student(int(student_id), full_name, class_name, email, phone))
            inserted_enrollment_block = fresh_manager.insert_enrollment(
                Enrollment(int(student_id), int(course_id), semester, float(score))
            )
            after_student_blocks = fresh_manager.student_blocks
            after_partition_blocks = fresh_manager.semester_partitions.get(semester, [])
            st.caption(
                f"Student chèn vào block {inserted_student_block + 1}, enrollment chèn vào phân vùng {semester} block {inserted_enrollment_block + 1}."
            )
            st.markdown("#### Trước khi chèn")
            render_block_grid("Partitioning - khối sinh viên", before_student_blocks, preview_head=3, preview_tail=3)
            render_block_grid(
                f"Partitioning - phân vùng {semester}",
                before_partition_blocks,
                preview_head=3,
                preview_tail=3,
            )
            st.markdown("#### Sau khi chèn")
            render_block_grid(
                "Partitioning - khối sinh viên",
                after_student_blocks,
                inserted_block=inserted_student_block,
                preview_head=3,
                preview_tail=3,
            )
            render_block_grid(
                f"Partitioning - phân vùng {semester}",
                after_partition_blocks,
                inserted_block=inserted_enrollment_block,
                preview_head=3,
                preview_tail=3,
            )
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Bố cục block hiện tại</div>", unsafe_allow_html=True)
    render_block_grid("Partitioning - khối sinh viên", manager.student_blocks, preview_head=4, preview_tail=4)
    for semester_name, blocks in manager.semester_partitions.items():
        st.markdown(f"<div class='section-subtitle'>Học kỳ {html.escape(semester_name)}</div>", unsafe_allow_html=True)
        render_block_grid(f"Partitioning - {semester_name}", blocks, preview_head=3, preview_tail=3)
    st.markdown("</div>", unsafe_allow_html=True)


def render_comparison_tab(data_dir: Path, results_dir: Path, block_capacity: int, compare_student_id: int, compare_semester: str | None) -> None:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>So sánh cả 4 kiểu tổ chức file</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-subtitle'>Dùng cùng một truy vấn để so sánh block I/O và thời gian xử lý.</div>", unsafe_allow_html=True)
    st.caption(f"Student ID: {compare_student_id} · Học kỳ: {compare_semester if compare_semester else 'không lọc'}")
    run_compare = st.button("Chạy so sánh & lưu báo cáo", type="primary")
    if run_compare:
        semester_filter = compare_semester.strip() or None
        rows = get_cached_benchmark_rows(str(data_dir), int(compare_student_id), semester_filter, block_capacity)
        st.dataframe(
            [
                {
                    "Phương pháp": row.method,
                    "I/O sinh viên": row.student_reads,
                    "I/O enrollment": row.enrollment_reads,
                    "Tổng I/O": row.total_reads,
                    "Thời gian (ms)": round(row.elapsed_ms, 2),
                }
                for row in rows
            ],
            use_container_width=True,
            hide_index=True,
        )
        report_sections = [
            "THÔNG TIN CHẠY",
            f"- data_dir: {data_dir}",
            f"- student_id: {compare_student_id}",
            f"- semester: {semester_filter if semester_filter is not None else 'không lọc'}",
            f"- block_capacity: {block_capacity}",
            "",
            format_benchmark_table(rows, title="So sánh I/O khối"),
        ]
        report_content = "\n".join(report_sections)
        run_file, latest_file = save_report(results_dir, report_content)
        st.success(f"Đã lưu báo cáo: {run_file.name}")
        st.caption(f"Bản mới nhất: {latest_file.name}")
    st.markdown("</div>", unsafe_allow_html=True)

    cached_managers = get_cached_managers(str(data_dir), block_capacity)
    semester_filter = compare_semester.strip() or None
    rows = get_cached_benchmark_rows(str(data_dir), int(compare_student_id), semester_filter, block_capacity)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Bảng so sánh nhanh</div>", unsafe_allow_html=True)
    st.dataframe(
        [
            {
                "Phương pháp": row.method,
                "I/O sinh viên": row.student_reads,
                "I/O enrollment": row.enrollment_reads,
                "Tổng I/O": row.total_reads,
                "Thời gian (ms)": round(row.elapsed_ms, 2),
            }
            for row in rows
        ],
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Heap thường chậm hơn khi đọc; Sequential tốt hơn cho tìm khóa; Clustering tối ưu khi đọc dữ liệu liên quan; Partitioning mạnh nhất khi lọc đúng khóa phân vùng.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Xem nhanh block của từng kiểu</div>", unsafe_allow_html=True)
    for kind in MANAGER_ORDER:
        with st.expander(kind, expanded=kind == "Heap"):
            manager = cached_managers[kind]
            if kind in {"Heap", "Sequential"}:
                render_block_grid(f"{kind} - sinh viên", manager.student_blocks, preview_head=3, preview_tail=3)
                render_block_grid(f"{kind} - enrollment", manager.enrollment_blocks, preview_head=3, preview_tail=3)
            elif kind == "Clustering":
                render_block_grid(f"{kind} - các cụm", manager.cluster_blocks, preview_head=3, preview_tail=3)
            elif kind == "Partitioning":
                render_block_grid(f"{kind} - sinh viên", manager.student_blocks, preview_head=3, preview_tail=3)
                for semester_name, blocks in manager.semester_partitions.items():
                    st.markdown(f"<div class='section-subtitle'>Học kỳ {html.escape(semester_name)}</div>", unsafe_allow_html=True)
                    render_block_grid(f"{kind} - {semester_name}", blocks, preview_head=2, preview_tail=2)
    st.markdown("</div>", unsafe_allow_html=True)


def sidebar_controls() -> tuple[int, str]:
    with st.sidebar:
        st.markdown("### Thiết lập demo")
        block_capacity = st.number_input("Dung lượng khối", min_value=1, value=15, step=1, key="sidebar_block_capacity")
        display_mode = st.selectbox(
            "Chế độ hiển thị block",
            options=["focus", "all"],
            format_func=lambda x: "1) Chỉ 2 block chứa dữ liệu tìm kiếm" if x == "focus" else "2) Toàn bộ block",
            index=0,
            key="sidebar_display_mode",
        )
        st.markdown("---")
        st.caption("Dataset mặc định dùng thư mục data.")
    return int(block_capacity), display_mode


def validate_dataset(data_dir: Path) -> bool:
    required_files = [data_dir / "students.txt", data_dir / "courses.txt", data_dir / "enrollments.txt"]
    missing = [path.name for path in required_files if not path.exists()]
    if missing:
        st.error("Không tìm thấy dataset: " + ", ".join(missing))
        return False
    return True


def main() -> None:
    page_styles()
    app_header()

    data_dir = Path("data")
    block_capacity, display_mode = sidebar_controls()
    if not validate_dataset(data_dir):
        st.stop()

    managers = get_cached_managers(str(data_dir), block_capacity)

    left_col, right_col = st.columns([2.2, 1], gap="large")
    with right_col:
        query_student_id, query_semester = render_query_panel(5, "2024A")
    with left_col:
        render_demo_grid(managers, query_student_id, query_semester, display_mode)

    render_comparison_table(data_dir, block_capacity, query_student_id, query_semester)


if __name__ == "__main__":
    main()
