# Trạng thái kiểm chứng

## Kiểm tra lại ngày 13/09/2026

Kết luận: phần xử lý dữ liệu chạy được trên Spark/Iceberg local với dữ liệu kiểm thử; chưa xác nhận toàn bộ deployment Docker hoặc toàn bộ CSV nguồn.

| Kiểm tra chạy lại | Kết quả |
|---|---|
| `bash scripts/test.sh` | Ruff pass; 26 unit tests pass, 3 integration tests deselected |
| `.venv/bin/pip check` | Không có dependency bị hỏng |
| `.venv/bin/dbt parse --project-dir dbt --profiles-dir dbt` | Pass, dbt-core/dbt-spark 1.11.0 |
| `.venv/bin/python scripts/run_spark_tests.py` | 1 pass, 2 skip; scenario mất 63,63 giây |
| Header 5 CSV nguồn (~2,3 GiB) | Đúng hợp đồng cột; chưa kiểm tra toàn bộ nội dung |
| `bash -n` cho 9 shell scripts | Pass |
| `python3 scripts/bootstrap.py --check` | Pass |

Spark scenario xác nhận replay không nhân đôi, dữ liệu đến muộn, doanh thu/funnel/RFM, quarantine, full refresh sau retirement và sinh dbt catalog. Bốn build hợp lệ đều có 29 node pass; build đặt ngưỡng quarantine bằng 0 thất bại đúng dự kiến ở hai quality tests. Log của lần chạy nằm trong `artifacts/integration/`. Hai test PostgreSQL bị skip vì chưa cấu hình database kiểm thử; không tính là pass.

Các trở ngại môi trường tại lần kiểm tra này:

- Docker chưa khả dụng trong WSL, nên không chạy được Compose và kiểm thử toàn stack.
- Thiếu GNU Make; đã gọi trực tiếp `scripts/test.sh` để kiểm thử.
- WSL có khoảng 5,6 GiB RAM, thấp hơn ngân sách core 12 GiB/toàn stack 16 GiB.
- Môi trường Conda `ETL` có Airflow 3.3.1, trong khi Dockerfile/CI ghim 3.1.7. Chạy `scripts/check_dags.py` trong môi trường này lỗi `DagBag.__init__() got an unexpected keyword argument 'include_examples'`. Đây là kiểm tra trên phiên bản khác cấu hình dự án; lần này chưa xác nhận lại DAG trên 3.1.7.

Muốn nghiệm thu deployment: bật Docker cho WSL, cấp đủ RAM, cài GNU Make rồi chạy `make demo` và `make status` theo README. Kết quả ngày 11/09 dưới đây là lịch sử, không phải các kiểm tra đã chạy lại toàn bộ ngày 13/09.

## Lịch sử kiểm chứng ngày 11/09/2026

Ngày thực hiện: **11/09/2026**. Kết quả này phân biệt rõ code đã kiểm thử với deployment chưa được nghiệm thu.

## Đã chạy thành công tại workspace

| Kiểm tra | Kết quả |
|---|---|
| Ruff trên runtime, DAG, scripts và tests | Pass |
| Python unit tests | 19 pass |
| dbt parse với dbt-core/dbt-spark 1.11.0 | Pass |
| Import và serialize DAG bằng Airflow 3.1.7 | Pass; đủ 5 task và publication phụ thuộc transform |
| Docker Compose 2.39.4 config validation | Pass |
| Image tags được tra trực tiếp trên Docker Registry | Các image được chọn tồn tại |
| JAR dependencies | SHA-256 được ghi cố định và đối chiếu |
| PostgreSQL registry integration | 2 pass trên PostgreSQL 16.15 native, database UTF-8 |
| Spark/Iceberg/dbt integration | 1 scenario pass trên Spark 3.5.7 + Iceberg 1.10.0, Hadoop catalog local |

Kịch bản Spark chạy model SQL thật và dbt build/tests thật:

1. Load cùng một file hai lần → Bronze vẫn 12 dòng.
2. Silver còn 10 sự kiện hợp lệ duy nhất; 1 record quarantine.
3. Khi duplicate có thuộc tính khác nhau, tie-breaker chọn kết quả ổn định. Tổng doanh thu bằng 160, funnel có đúng thứ tự và xử lý qua nửa đêm; RFM của khách tốt nhất có điểm (5,5,5).
4. dbt build lại → không tăng số dòng Silver.
5. Thêm file chứa một duplicate và purchase ngày cũ → Silver 11, doanh thu 165.
6. Ngưỡng lỗi đặt bằng 0 → hai quality tests thất bại đúng dự kiến, dbt trả exit code khác 0.
7. Retirement file lỗi rồi full refresh → Silver còn đúng 2 sự kiện của file còn lại, doanh thu 15.
8. `dbt docs generate` tạo catalog artifact từ các bảng Iceberg thật.

Mỗi build hợp lệ gồm 5 model vật lý và 24 data tests (29 node pass). Scenario kiểm tra cả kết quả số liệu ngoài dbt nên không chỉ dựa vào test not_null. Logs được sinh tại `artifacts/integration/` và không commit dữ liệu/log vào Git.

Unit tests còn mô phỏng insert ClickHouse lỗi sau khi đã ghi một phần: release cũ không đổi, retry không nhân đôi dữ liệu; fingerprint sai chặn publication. PostgreSQL tests dùng database thật, còn object storage trong hai test đó là test double.

## Chưa được xác nhận tại workspace

- Docker Engine chưa khả dụng trong WSL; host hiện có khoảng 5,6 GiB RAM, thấp hơn ngân sách core 12 GiB/toàn stack 16 GiB.
- Chưa build/run toàn bộ container stack, chưa nghiệm thu MinIO + Iceberg JDBC catalog + ClickHouse + Superset cùng lúc.
- Chưa thực hiện EXCHANGE trên ClickHouse server thật, UI dashboard/filter thực tế hoặc backup/restore Docker volumes.
- Chưa benchmark toàn bộ 5 CSV (~2,3 GiB), chưa đo RAM đỉnh, throughput, RPO/RTO hoặc kiểm thử failover.
- CI workflow đã được viết nhưng chưa chạy trên GitHub trong phiên này.

Do đó, không ghi “đã deploy production thành công”. Để nghiệm thu, bật Docker trên máy Linux đủ tài nguyên, chạy `make demo` rồi hoàn thành checklist trong [deployment.md](deployment.md). Chỉ ghi thêm ảnh, benchmark và SLA sau khi có kết quả thực tế.

## Những lỗi đã bắt được bằng kiểm thử

- Namespace default phải tồn tại trước khi PyHive mở session.
- Iceberg catalog cache trong Thrift chạy lâu có thể làm các bước đọc snapshot cũ; đã tắt catalog cache.
- PyHive trả DATE dạng chuỗi; publisher chuyển sang date object trước khi ClickHouse binary insert.
- Dialect SQLAlchemy thực tế của clickhouse-connect là `clickhousedb`, đã sửa connection Superset.
- Một số Maven artifacts không xuất bản checksum .sha512; dùng manifest SHA-256 cố định trong repo.
- dbt schema vars phải có default ngay lúc render project config.

## Dọn cấu trúc

Đã bỏ các job Spark/JDBC append đời cũ, ClickHouse init SQL placeholder, SQL smoke/sample không còn dùng, script bootstrap cũ và tài liệu chỉ lặp lại tài liệu chính. Các cấu hình ClickHouse còn dùng nằm trong `infra/clickhouse/`. Dữ liệu nguồn và `.env` cũ được giữ nguyên.
