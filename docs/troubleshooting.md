# Troubleshooting

| Triệu chứng | Kiểm tra và xử lý |
|---|---|
| Docker không tìm thấy trong WSL | Bật Docker Desktop integration cho distro hoặc cài Docker Engine trên Linux; chạy docker info |
| Preflight báo thiếu RAM | Tăng RAM WSL/VM; core 12 GiB, toàn stack mục tiêu 16 GiB |
| PyHive báo schema default không tồn tại | Spark startup phải chạy CREATE NAMESPACE lakehouse.default trước Thrift |
| ClassNotFound S3FileIO/JDBC/S3A | Build spark/Dockerfile; không bind đè toàn bộ /opt/spark/jars bằng thư mục rỗng |
| Checksum JAR sai | Xóa đúng file cache tải hỏng, tải lại qua download_jars.sh; không bỏ qua checksum |
| dbt không tìm profile | Chạy bootstrap.py, image cũng copy profiles.yml.example khi build |
| dbt schema bị lặp tên | Dùng macro generate_schema_name trong repo, không dùng macro mặc định với custom schema |
| Bronze đọc thiếu cột corrupt | Dùng schema explicit với _corrupt_record; tắt CSV column pruning; không infer schema |
| Dữ liệu lỗi vượt ngưỡng | Xem quarantine_events và lý do, sửa nguồn/contract rồi replay có kiểm soát |
| Airflow run cũ bị từ chối | Đã có run mới hơn; trigger run mới thay vì Clear một run lịch sử |
| Dashboard chưa có charts | Đợi DAG thành công rồi make dashboard |
| Login sau đổi env không được | Account/role lưu trong metadata DB; startup không tự đổi mật khẩu đã tồn tại |
| Doanh thu không bằng funnel purchases | Funnel chỉ đếm chuỗi view/cart/purchase hợp lệ, sales tính mọi purchase event |

Xem [operations.md](operations.md) cho retry/rollback và [validation.md](validation.md) cho phạm vi đã kiểm thử.
