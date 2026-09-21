# Trình bày dự án bằng tình huống vận hành

## Câu chuyện chính

“Pipeline không chỉ tính ra một dashboard. Nó phải giải thích dữ liệu đến từ đâu, chịu được retry, sửa dữ liệu đến muộn và không công bố kết quả sai.”

1. Mở sơ đồ kiến trúc. Giải thích vì sao raw được load trước transform và vì sao một máy không cần giả lập cluster phức tạp.
2. Mở Airflow Graph và ops run report. Đi từ run ID tới file SHA rồi raw object; mở artifacts dbt để xem lineage và tests.
3. Trình bày test replay: nạp cùng CSV hai lần, số dòng Bronze không nhân đôi; dedup xuyên file ở Silver.
4. Trình bày late arrival: một purchase của ngày cũ làm doanh thu ngày cũ cập nhật; tránh watermark chỉ theo ngày lớn nhất.
5. Trình bày lỗi: quality budget thất bại hoặc insert ClickHouse dở; current_release cũ vẫn phục vụ dashboard.
6. Mở Superset với doanh thu, funnel theo thứ tự, RFM. Giải thích vì sao không gọi purchase count là số đơn hàng.
7. Mở Grafana cho thời gian thành công gần nhất, quarantine, lỗi và stage bị kẹt; nói rõ receiver cảnh báo ngoài hệ thống chưa cấu hình.
8. Kết thúc bằng đánh đổi: full scan dễ kiểm chứng, một writer dễ quản lý; chỉ tối ưu khi đã có benchmark và oracle đối soát.

## Bằng chứng nên lưu

- Log test first_build/replay_build/late_build/quality_gate.
- `manifest.json`, `run_results.json`, `catalog.json`, `run.json` trong prefix artifacts.
- Ảnh Airflow run thành công; ảnh run thất bại nhưng dashboard vẫn đọc release cũ.
- Kết quả SQL tổng doanh thu Silver/Gold/serving bằng nhau.
- Kết quả restore backup và rollback release.

Không đưa benchmark hoặc ảnh dashboard mẫu chưa chạy thành công vào README như bằng chứng thực tế. `make demo` là script nghiệm thu có chờ run thành công, không phải animation mô phỏng.
