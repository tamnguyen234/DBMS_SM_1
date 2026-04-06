# Demo Quản Lý Lưu Trữ

Mô phỏng bằng Python cho bốn kiểu tổ chức file thường dùng trong quản lý lưu trữ của hệ quản trị CSDL:

- Tệp Heap
- Tệp tuần tự
- Gom cụm đa bảng
- Phân vùng theo học kỳ

## Cấu Trúc Dự Án

- `data/`: các file TXT có cấu trúc bảng, gồm `students`, `courses` và `enrollments`
- `models/`: các dataclass cho `Student`, `Course` và `Enrollment`
- `engines/`: mô phỏng tổ chức file và đếm I/O khối
- `scripts/`: bộ sinh dataset
- `benchmarks/`: các hàm so sánh
- `results/`: lưu báo cáo mỗi lần chạy benchmark
- `main.py`: điểm vào để chạy demo

## Sinh Dataset

```bash
python scripts/generate_dataset.py --output-dir data --student-count 1000000 --course-count 500 --enrollments-per-student 1
```

Lệnh này sẽ tạo:

- `data/students.txt`
- `data/courses.txt`
- `data/enrollments.txt`

## Chạy Demo

### Demo giao diện Streamlit

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Giao diện có các tab riêng cho Heap, Sequential, Clustering, Partitioning và một tab so sánh tổng hợp. Mỗi tab đều có phần trực quan hóa block trước/sau khi chèn và phần demo tra cứu.

### Demo dòng lệnh (CLI)

```bash
python main.py --data-dir data --student-id 1000
```

CLI dùng `rich` để in bảng I/O trực quan hơn trên terminal.

Bộ lọc học kỳ tùy chọn khi tra cứu bản ghi đăng ký học phần:

```bash
python main.py --data-dir data --student-id 1000 --semester 2024A
```

Mỗi lần chạy, chương trình tự động ghi báo cáo vào thư mục `results/`:

- `results/bao_cao_benchmark_YYYYMMDD_HHMMSS.txt`: báo cáo đầy đủ của lần chạy hiện tại
- `results/bao_cao_benchmark_moi_nhat.txt`: báo cáo đầy đủ mới nhất (được ghi đè mỗi lần chạy)

Bạn có thể đổi thư mục lưu kết quả bằng tham số:

```bash
python main.py --data-dir data --student-id 1000 --results-dir results
```

Khi truyền `--semester`, cách tổ chức phân vùng chỉ quét đúng phân vùng của học kỳ đó, nên chênh lệch I/O sẽ dễ quan sát hơn.

## Ghi Chú

- Kích thước khối được mô phỏng bằng `--block-capacity`.
- Mỗi truy vấn đều trả về số khối đã đọc, đây là chỉ số chi phí I/O chính.
- `main.py` cũng hiển thị phần xem trước trạng thái khối trước và sau khi chèn trên bố cục Heap.
