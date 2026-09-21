# Data contract — commerce events v2

## Nguồn và phạm vi

Dữ liệu hiện có: 5 CSV tháng 10/2019–02/2020 trong `data/rawdata/`, khoảng 2,3 GiB. Kiểm tra mẫu mỗi file xác nhận 4 loại sự kiện, không phải purchase-only. Contract xử lý UTF-8/UTF-8 BOM, header có thể đảo thứ tự; một record trên một dòng, không hỗ trợ trường CSV chứa xuống dòng.

File phải hoàn chỉnh trước khi vào input. File thay đổi có SHA khác và được xem như nguồn sự kiện bổ sung. Khi cần thay thế một file lỗi đã nạp, dùng retirement có audit và full rebuild theo [runbook](operations.md#thay-thế-file-nguồn-bị-lỗi); không chỉ ghi đè filename.

| Cột nguồn | Kiểu Bronze | Quy tắc Silver |
|---|---|---|
| event_time | string | Timestamp UTC, chấp nhận hậu tố ` UTC`; không parse được → quarantine |
| event_type | string | trim/lower; view, cart, remove_from_cart, purchase |
| product_id | string | bigint > 0 |
| category_id | string | bigint nullable; không parse được khi có giá trị → quarantine |
| category_code | string | lower/trim; null/rỗng → unknown |
| brand | string | lower/trim; null/rỗng → unknown |
| price | string | decimal(18,2), >=0; thiếu/không hợp lệ/âm → quarantine |
| user_id | string | bigint > 0 |
| user_session | string | trim, không null/rỗng |
| payment_method | string, tùy chọn | nullable; lower/trim |

Header thiếu cột bắt buộc, trùng tên hoặc có cột chưa khai báo làm stage fail. Bản raw vẫn được giữ để điều tra. Cần review contract và migration trước khi thêm trường mới.

## Grain và định danh

- Raw: một object trên một nội dung file duy nhất; SHA-256 không phụ thuộc tên file.
- Bronze: giữ mọi record đọc được, kể cả duplicate và malformed row; `_corrupt_record`, `_file_sha256`, `_source_uri`, `_ingested_at`, `_run_key` truy vết về raw. Partition theo SHA file.
- Silver: một sự kiện theo khóa hash của `user_id, user_session, product_id, event_ts, event_type, price` sau chuẩn hóa. Các record cùng khóa chọn ingestion mới nhất, rồi file SHA và các thuộc tính category/brand/payment_method làm tie-breaker ổn định.
- Do không có event ID nguồn, hai sự kiện hợp lệ giống hệt tất cả trường khóa không thể phân biệt và sẽ gộp. Không coi đây là bảo đảm dedup tuyệt đối theo sự kiện thực tế. Một correction đổi price cũng tạo khóa khác.
- Quarantine: một record Bronze bị loại với `rejection_reason`; giữ các giá trị thô. Trước công bố, tỷ lệ lỗi toàn bộ **và mỗi file** phải <= `max_invalid_ratio` (mặc định 1%).

## Gold

### daily_sales_by_category

Grain: `(event_date, category_code, brand)`. `revenue` là sum price của purchase event; `purchase_events` là count sự kiện; `purchasing_sessions` là count distinct `(user_id, user_session)` **trong nhóm**; `avg_purchase_event_value` là revenue / purchase_events.

Không có order_id, quantity, thuế, refund, phí vận chuyển hoặc mã tiền tệ đủ để suy ra doanh thu tài chính/GMV chuẩn. Không gọi purchase_events là đơn hàng, không gọi giá trị trung bình này là AOV đơn hàng. Số phiên mua không được cộng qua danh mục vì một phiên có thể thuộc nhiều nhóm. Trung bình cũng không được cộng hoặc lấy trung bình không trọng số.

### funnel_steps_daily

Grain: ngày bắt đầu phiên `(user_id, user_session)` theo sự kiện sớm nhất quan sát được. Tìm view đầu tiên, cart đầu tiên sau view, purchase đầu tiên sau cart. Cho phép timestamp bằng nhau do nguồn chỉ có độ phân giải giây. Phiên qua nửa đêm vẫn thuộc ngày bắt đầu.

`sessions_total >= sessions_view >= sessions_cart_after_view >= sessions_purchase_after_cart`; các tỷ lệ nằm trong [0,1], mẫu số 0 cho kết quả 0. Một purchase không có chuỗi view/cart hợp lệ vẫn tính trong Sales nhưng không tính chuyển đổi funnel. Remove-from-cart được giữ trong Silver, không phải bước funnel; chưa mô hình hóa trạng thái giỏ theo sản phẩm.

### rfm_segments

Grain: một user tại ngày snapshot được chọn. Mặc định snapshot_date là max(event_date) của Silver; có thể chỉ định ngày qua tham số DAG. Chỉ purchase không muộn hơn snapshot được tính. Đây là bảng snapshot hiện tại, lịch sử các lần công bố còn trong ClickHouse releases/Iceberg snapshots theo retention.

Recency: số ngày từ lần mua gần nhất. Frequency: số phiên có mua. Monetary: tổng price purchase. `5-floor(4*percent_rank())` với chiều sắp xếp phù hợp cho điểm 5 tốt nhất, 1 thấp nhất; ties nhận cùng điểm. Một tập chỉ có một giá trị nhận 5; đây không phải chia năm nhóm bằng số lượng như ntile.

Các nhãn champions, loyal, at_risk, hibernating, others là quy tắc phân tích mẫu, chưa được hiệu chỉnh theo chiến dịch marketing hay giá trị vòng đời khách hàng thật.

## Khác biệt so với v1

Catalog `rest` → `lakehouse`; ClickHouse `analytics` → `cosmetics_analytics`; bỏ tên orders/aov và funnel đếm user không theo thứ tự. Không migrate trực tiếp bảng v1. Triển khai v2 vào volumes/bucket mới, replay raw, đối soát rồi mới chuyển người dùng dashboard. Không xóa dữ liệu v1 trong quá trình này.
