# Runbook vận hành

## Run, retry và backfill

DAG `cosmetics_elt`: land → bronze → transform → publish → finish. Mỗi run có manifest file cố định. Airflow retry 2 lần với backoff, timeout task 3 giờ, timeout DAG 8 giờ. Pool `lakehouse_writes` chỉ có một slot trên host.

- Lỗi tạm thời: mở task log trong Airflow, sửa nguyên nhân, Clear task lỗi và các downstream task của **run mới nhất**. Giữ nguyên run ID và params.
- File mới đến sau khi land đã thành công: trigger run mới; không thay manifest run cũ.
- Backfill: đặt CSV lịch sử hoàn chỉnh vào input rồi trigger run mới. Gold tính lại toàn bộ nên cập nhật được ngày cũ.
- Chỉ sửa SQL/config: rebuild image, trigger run mới. Không có file mới vẫn chạy transform/publication để áp dụng logic đã review.
- Nếu đã có run mới hơn, không Clear run cũ. Dùng run mới để tránh công bố ngược thứ tự.
- Thay khóa dedup hoặc thay chính sách loại record: review migration, rebuild image rồi Trigger với `{"full_refresh":true}` để dựng lại Silver; MERGE thông thường không retract row từng hợp lệ nhưng nay bị loại.

CLI tương đương khi không có DAG đang chạy:

```bash
docker compose --env-file .env.deploy exec -T airflow-scheduler \
  /opt/pipeline-venv/bin/python -m lakehouse.cli run \
  --project cosmetics --run-id manual_review_001 --as-of-date 2020-02-29
```

Không chạy CLI xen giữa các task của một DAG run khác. `max_active_runs` bảo vệ Airflow; advisory lock bảo vệ từng stage và công cụ quản trị, không biến thao tác CLI tùy ý thành distributed scheduler.

## Điều tra chất lượng

```sql
-- Spark SQL
SELECT rejection_reason, count(*)
FROM cosmetics_silver.quarantine_events GROUP BY rejection_reason;
SELECT _file_sha256, rejection_reason, count(*)
FROM cosmetics_silver.quarantine_events GROUP BY _file_sha256, rejection_reason;
SELECT sum(price) FROM cosmetics_silver.stg_events WHERE event_type='purchase';
SELECT sum(revenue) FROM cosmetics_gold.daily_sales_by_category;
```

Không tự nâng ngưỡng quarantine để làm test xanh. Xác nhận lỗi thuộc nguồn, parsing hay contract; lưu raw mẫu trong vùng bảo vệ và cập nhật test. Row ngoài ngưỡng khiến run fail trước publication. Header chưa được hỗ trợ cũng fail; raw object vẫn có để điều tra.

`ops.files` ánh xạ SHA → object key, tên gốc, số byte, header, checkpoint Bronze. `ops.run_files` là manifest. `ops.stages` lưu mọi attempt. `ops.runs.metrics` lưu counts và fingerprint candidate. Chi tiết lỗi ở Airflow logs; ops chỉ lưu loại exception để tránh vô tình lưu credential từ driver.

Artifacts: `s3://<bucket>/artifacts/<project>/<run_key>/dbt/` và `run.json`. `make status` kiểm tra container bắt buộc, health check, init exit code, DAG import và DAG đã đăng ký rồi in 10 run gần nhất. Dịch vụ tùy chọn cũng được kiểm tra nếu container đã tồn tại. Lệnh trả exit code khác 0 nếu hạ tầng hoặc DAG chưa sẵn sàng; lịch sử run cần được đọc riêng để xác nhận pipeline thành công. Superset/Grafana là hai góc nhìn khác nhau: kết quả phân tích và vận hành pipeline.

## Thay thế file nguồn bị lỗi

Nếu một file đã vào Bronze vượt quality budget, chỉ thêm file đã sửa chưa đủ: các record cũ vẫn còn trong Bronze. Thực hiện retirement có lý do, giữ raw để audit:

```bash
# Pause DAG, chờ run hiện tại kết thúc; lấy SHA từ ops.files/quarantine.
docker compose --env-file .env.deploy exec -T airflow-scheduler \
  /opt/pipeline-venv/bin/python -m lakehouse.cli retire-file \
  --project cosmetics --file-sha <sha256> --reason "Nguồn đã được đối chiếu, thay bằng file sửa lỗi"
# Xem kế hoạch, thêm --apply để ghi retirement.
```

Đưa file thay thế đã sửa vào input, trigger **run mới**. Bronze xóa partition của file retired; raw object vẫn giữ nguyên. Transform tự chạy full-refresh vì có retirement chưa xử lý, chỉ clear cờ rebuild sau khi dbt build/tests thành công. File retired vẫn nằm trong ops với thời điểm/lý do, không tự nạp lại dù còn trong input. Đây là replacement theo file có kiểm soát, không phải CDC theo event ID. Không retire toàn bộ nguồn mà không có file thay thế: empty Bronze bị chặn.

## Rollback serving

Pause DAG và đợi hết run đang chạy. Liệt kê release:

```bash
docker compose --env-file .env.deploy exec -T airflow-scheduler \
  /opt/pipeline-venv/bin/python -m lakehouse.releases --project cosmetics
```

Chạy lại với `--release <run_key>` để kiểm tra candidate manifest và fingerprint dữ liệu còn lưu. Thêm `--apply` để đổi pointer nguyên tử; thao tác được audit. Không cần xóa Gold/Iceberg hay chạy lại ingest để rollback dashboard. Run thành công tiếp theo sẽ công bố release mới, vì vậy giữ DAG paused trong lúc điều tra.

Rollback bị từ chối nếu retention đã xóa dữ liệu hoặc fingerprint không khớp. Release pointer chỉ điều khiển serving, không rollback mã nguồn, metadata migration hoặc Iceberg source.

## Retention

```bash
# Xem danh sách trước, mặc định giữ ít nhất 7 release và 7 ngày
docker compose --env-file .env.deploy exec -T airflow-scheduler \
  /opt/pipeline-venv/bin/python -m lakehouse.retention --project cosmetics
# Thêm --apply sau khi kiểm tra danh sách.
```

Công cụ không xóa active release, dùng cùng advisory lock với publisher và ghi audit. Không đặt TTL tự động lên active partition. Tăng thời gian giữ nếu dữ liệu cần audit dài hơn.

Iceberg snapshots cũng tăng theo mỗi commit. Sau khi có backup, chọn retention phù hợp rồi chạy `CALL lakehouse.system.expire_snapshots(...)` trên từng bảng trong maintenance window; giữ đủ snapshots cho điều tra. Không chạy remove_orphan_files với thời gian quá gần vì có thể đụng file của writer đang hoạt động. Raw và ops không có cleanup tự động trong bản này. MinIO versioning không thay thế backup ngoài máy. Airflow log retention cần cron/housekeeping theo chính sách; Docker stdout đã có rotation 10 MiB × 3.

## Backup nhất quán có downtime

Bản một máy dùng cold backup toàn bộ named volumes, gồm PostgreSQL/catalog, MinIO, ClickHouse, metadata BI, logs/artifacts và monitoring. Dừng writer giúp catalog và object metadata cùng một thời điểm.

1. Pause tất cả DAG; chờ mọi run kết thúc. Dừng luồng đưa file mới vào input trong maintenance window.
2. Chạy `python3 scripts/backup.py create --stop-services`. Script từ chối khi ops còn run running, dừng stack, nén volumes, ghi SHA-256 và khởi động lại đúng các service trước đó đang chạy.
3. Lưu `.env.deploy` riêng trong kho secrets được mã hóa; giữ commit và image versions trong manifest. Không đưa secrets vào Git.
4. Copy thư mục backup ra máy/storage khác. Backup nằm cùng ổ đĩa không bảo vệ khi mất host.
5. Nghiệm thu bằng restore vào project/máy riêng.

Thao tác backup có downtime và yêu cầu Docker tải được alpine:3.22.1. Nếu dừng giữa chừng, archive chưa có manifest hoàn chỉnh không được coi là backup hợp lệ. Dữ liệu đầu vào chưa land trong thư mục host không nằm trong named volumes; sao lưu thư mục đó bằng quy trình nguồn riêng.

## Restore thử, không đè môi trường đang chạy

```bash
python3 scripts/backup.py restore \
  --backup backups/<timestamp> --target-project cosmetics_restore
```

Công cụ kiểm tra checksum mọi archive trước khi tạo volumes, từ chối nếu target volume đã tồn tại, và không tự khởi động stack. Chỉ dùng archive do chính quy trình backup đáng tin cậy tạo.

Tạo `.env.restore` từ secrets tương ứng backup, đổi port để không xung đột, giữ S3 bucket và mật khẩu catalog đúng như backup. Khởi động bằng `docker compose --env-file .env.restore -p cosmetics_restore --profile bi --profile observability up -d`. Dùng code/images trong manifest; không nâng phiên bản trong lúc phục hồi. Giữ các DAG paused, xác nhận query Iceberg, current_release và dashboard trước khi bật lịch.

RPO phụ thuộc lịch backup; RTO phải đo bằng restore drill. Repo không đưa ra SLA khi chưa có số liệu này.

## Cảnh báo và xử lý sự cố

Prometheus có rules: exporter mất kết nối, run mới nhất thất bại, quá 26 giờ chưa có run thành công, stage running quá 3 giờ. Grafana có dashboard theo project. Hiện cảnh báo hiển thị trong Prometheus, **chưa gửi email/Slack/webhook**; cấu hình Alertmanager/receiver của tổ chức trước khi dùng cho SLA.

Nếu worker bị kill không ghi được failure vào ops, stage có thể vẫn running; cảnh báo stage age giúp phát hiện. Đối chiếu Airflow task state, retry task; không tự sửa DB checkpoint để bỏ qua bước kiểm tra.
