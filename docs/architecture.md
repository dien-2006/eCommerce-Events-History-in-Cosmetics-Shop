# Các quyết định kiến trúc

## ADR-001 — Load raw trước, transform sau

Raw object giữ nguyên byte CSV tại `raw/<project>/sha256=<hash>/events.csv`. Snapshot tạm được tạo khi đọc nguồn để checksum và nội dung upload cùng một phiên bản. Chỉ đưa file đã ghi xong vào input bằng rename nguyên tử; không copy file đang thay đổi trực tiếp vào thư mục được quét.

Header được kiểm tra nhưng raw vẫn được lưu khi header lỗi. File có contract sai chưa vào manifest Bronze; sửa contract hoặc file rồi tạo run mới. Những cột mới không được âm thầm bỏ qua. `payment_method` là trường tùy chọn đã được khai báo, không phải hỗ trợ tự động mọi schema evolution.

## ADR-002 — Spark Thrift + Iceberg JDBC catalog trên một máy

Một Spark Thrift Server dùng `local[2]`, Iceberg JDBC catalog lưu bền trong PostgreSQL. dbt và loader SQL truy cập cùng catalog `lakehouse`. Không dùng REST catalog demo với trạng thái catalog không rõ; không dựng hai worker trên cùng máy chỉ để tăng số container.

Tách tài khoản/databases PostgreSQL cho Iceberg, Airflow, ops và Superset. MinIO giữ data/metadata files; backup catalog PostgreSQL và MinIO phải cùng thời điểm khi không có writer. Đây vẫn là single point of failure. Nếu tăng tải, thay điểm thực thi bằng Spark cluster/Kyuubi, giữ lại contract/SQL và đánh giá lại pool, quyền truy cập, giới hạn tài nguyên.

## ADR-003 — Idempotency theo từng ranh giới

| Ranh giới | Khóa | Cơ chế |
|---|---|---|
| File nguồn → raw | SHA-256 của byte | Cùng nội dung được nhận diện cùng file |
| Raw → Bronze | project + file SHA | Iceberg dynamic partition overwrite của đúng một file |
| Bronze → Silver | event_dedup_key | MERGE và row_number trên dữ liệu nguồn đã chuẩn hóa |
| Gold → ClickHouse | run_key/release_id | Xóa partition ứng viên chưa công bố, nạp lại, đối soát |
| Công bố | current_release | EXCHANGE một bảng pointer trong database Atomic |

Không có distributed transaction giữa PostgreSQL, Iceberg và ClickHouse. Vì vậy mỗi bước tự phục hồi khi chết giữa data commit và checkpoint. Cam kết là hiệu ứng idempotent trong mô hình **một writer/project**, không tuyên bố exactly-once xuyên mọi hệ thống.

Airflow `max_active_runs=1`, `max_active_tasks=1`, pool `lakehouse_writes=1`; PostgreSQL advisory lock bảo vệ mỗi stage. CLI cũng dùng khóa đó. Không chạy CLI xen kẽ các run của DAG; run cũ bị từ chối nếu một run mới hơn đã tồn tại. Việc giới hạn write slot toàn platform phù hợp ngân sách một máy và tránh dbt/Spark tranh RAM.

## ADR-004 — Tính đúng trước tối ưu incremental

Silver merge được tính trên toàn Bronze, Gold `CREATE OR REPLACE TABLE` trên toàn Silver. Điều này sửa late arrivals không giới hạn cửa sổ và dedup xuyên file, nhưng tốn scan. Với dữ liệu lớn hơn, cần đổi sang manifest các partition bị ảnh hưởng, recompute toàn partition, xử lý session xuyên ngày và chạy đối soát với full rebuild làm oracle. Không thay bằng `event_date > max(event_date)`: nó bỏ qua late data.

Nguồn hiện là append-only event history. Nếu upstream gửi correction hoặc delete, cần event ID ổn định, operation type và cơ chế retract; pipeline hiện tại không giả vờ hỗ trợ CDC. Thay đổi quy tắc loại trùng/loại dữ liệu phải dùng full_refresh sau review. Retirement theo file là ngoại lệ có audit: loại partition khỏi Bronze, giữ raw, buộc full rebuild Silver để retract đúng dữ liệu đã loại. Không chỉ thay SQL rồi merge vào bảng cũ.

## ADR-005 — Công bố là một bước riêng

Mỗi Gold mart có bảng `_versions_<name>` với `_release_id`, partition theo release. Nạp theo khối 10.000 dòng; Thrift bật incrementalCollect để không collect toàn bộ kết quả vào RAM driver. Fingerprint là tổng modulo 2^256 của hash từng dòng cộng số lượng dòng; thứ tự không ảnh hưởng, số bản ghi trùng vẫn ảnh hưởng. Decimal so sánh chính xác; Float64 chuẩn hóa 12 chữ số có nghĩa cho chỉ số tỷ lệ.

Sau khi so sánh toàn bộ dòng và lưu manifest ứng viên vào ops, đổi một `current_release` pointer bằng ClickHouse EXCHANGE. Các view ổn định tham chiếu pointer đó. Cache Superset bị tắt để không tiếp tục hiển thị release cũ do cache.

Đổi pointer là nguyên tử; nhiều query/chart riêng biệt đang chạy đúng thời điểm chuyển vẫn có thể nhìn hai release khác nhau. Không cam kết transaction snapshot cho cả dashboard nhiều query. Giữ `_release_id` cố định trong truy vấn phân tích đối soát khi cần consistency xuyên nhiều query.

Release cũ không bị tự động xóa; đổi lại rollback khả dụng. Retention cần operator chạy theo runbook, không dùng TTL có thể xóa active release khi pipeline dừng lâu.

## ADR-006 — Kiểm soát vận hành đủ dùng

Airflow cung cấp scheduler, UI, task logs và retry. `ops.runs`, `ops.files`, `ops.run_files`, `ops.stages` cung cấp metadata nghiệp vụ độc lập với thời gian giữ log của Airflow. dbt manifest/catalog/run_results được lưu theo run trên MinIO để xem lineage và test results. Prometheus đọc ops; Grafana so sánh giữa project.

Freshness pipeline là thời điểm run thành công; không nhầm với `latest_event_date` của dataset lịch sử. Chưa có source arrival SLA hoặc alert receiver external. Cần thiết lập theo nguồn thật trước khi vận hành dịch vụ liên tục.
