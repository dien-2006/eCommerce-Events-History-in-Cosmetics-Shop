# Triển khai và nâng cấp trên Linux

## Điều kiện

- Máy Linux amd64, 4 vCPU, RAM 16 GiB, SSD trống 50 GiB cho bản đầy đủ.
- Docker Engine đang hoạt động, Docker Compose >=2.39, Python 3.11/3.12, Make.
- Cho phép build tải image Docker Hub, package PyPI và JAR Maven Central.
- Port host mặc định: 8080, 8088, 9000, 9001, 4040, 8123, 3000, 9090. Không có xung đột MinIO/ClickHouse native 9000 như cấu hình v1.

`make up` thực hiện preflight, build, start và đợi health checks. Compose dùng images riêng chứa code và dependency; không pip install lúc khởi động. Build lại image sau khi đổi config, DAG, SQL hoặc Python.

## Môi trường v2 độc lập

`python3 scripts/bootstrap.py` sinh `.env.deploy`; giữ nguyên `.env` của v1. Tên Compose project mặc định `cosmetics_elt_v2`, bucket `cosmetics-lakehouse-v2`, volumes mới. Script không tự sửa environment đã tồn tại. Nếu đổi mật khẩu sau khi PostgreSQL đã init, phải đổi role trong DB tương ứng; thay env đơn thuần không cập nhật role.

Không đưa `.env.deploy`, raw CSV, backups hoặc log có dữ liệu vào Git. Repo v1 đã theo dõi `.env`; trước khi xuất bản repository, bỏ theo dõi file đó bằng `git rm --cached .env`, kiểm tra lịch sử và thay các credential đã từng công khai. File làm việc cũ được giữ trong quá trình sửa dự án này.

## Trình tự

1. Chạy `python3 scripts/bootstrap.py`, kiểm tra `.env.deploy` bằng editor.
2. Đặt nguồn trong `data/rawdata/`; bind mount được đặt read-only.
3. `make up`. Kiểm tra `make status`; Airflow DAG mặc định paused.
4. Trong Airflow bật `cosmetics_elt`, trigger run đầu và theo dõi từng task.
5. Khi run thành công, `make bi` và `make monitoring`.
6. Mở dashboard Superset, kiểm tra SQL đối soát trong runbook.
7. Nếu Superset đã khởi động trước data, chạy `make dashboard` để tạo charts.

Không bật lịch nhiều nguồn cho tới khi xác nhận run đầu. Lịch mặc định 02:00 UTC mỗi ngày, `catchup=False`; backfill được thực hiện bằng nạp file lịch sử và trigger run, không tạo hàng nghìn DAG run theo event date.

## Truy cập bên ngoài máy chủ

Giữ BIND_ADDRESS=127.0.0.1. Ví dụ tunnel: `ssh -L 8080:localhost:8080 -L 8088:localhost:8088 -L 3000:localhost:3000 user@host`. Nếu dùng domain thật, reverse proxy phải có TLS, cấu hình forwarded headers, xác thực/SSO phù hợp và firewall. Superset `force_https=false` phù hợp TLS termination hoặc localhost; cookie Secure cần bật khi triển khai HTTPS.

Spark Thrift NOSASL chỉ tồn tại trên mạng Compose nội bộ, không được mở ra Internet. Role `dashboard` của ClickHouse readonly và chỉ được đọc `cosmetics_analytics`. MinIO credential hiện dùng chung cho các worker nội bộ; tách service accounts/policies trước khi cho tenant hoặc nhóm không tin cậy truy cập.

## Nâng cấp

- Pause DAG, đợi run kết thúc, tạo backup nhất quán theo operations.md.
- Giữ lại image tags/digests và commit đang chạy; sửa từng nhóm phiên bản, không dùng latest.
- Chạy unit, dbt parse, DAG import và Spark integration, sau đó full-stack acceptance trên môi trường riêng.
- Rebuild: `docker compose --env-file .env.deploy build`.
- Khởi động lại bằng `make up`, kiểm tra migration Airflow/Superset và trigger một run có đối soát.
- Chỉ chuyển lịch thật sau khi dashboard đọc release mới đúng. Rollback serving bằng releases CLI không rollback schema ứng dụng; khi hỏng migration ứng dụng cần image/backup tương ứng.

## Nghiệm thu trước vận hành liên tục

- [ ] Toàn stack healthy; không có DAG import errors.
- [ ] Run đầu, retry cùng run và run mới với file cũ không tăng sai số dòng/doanh thu.
- [ ] File đến muộn cập nhật ngày lịch sử.
- [ ] Dữ liệu lỗi vượt ngưỡng làm fail và giữ nguyên current_release.
- [ ] Khôi phục một release cũ bằng rollback CLI và đối soát.
- [ ] Superset connection là dashboard readonly, charts/filters hoạt động.
- [ ] Backup đã restore trên máy/Compose project khác và truy vấn được Iceberg/ClickHouse.
- [ ] Đã có lưu backup ngoài máy chủ và receiver cảnh báo thật nếu dùng SLA.
- [ ] Đo thời gian run, RAM đỉnh và kích thước dữ liệu trên dataset đầy đủ; chưa có con số benchmark trong repo.
