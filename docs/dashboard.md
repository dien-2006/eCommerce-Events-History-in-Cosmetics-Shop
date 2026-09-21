# Superset v2

Sau khi DAG đầu tiên thành công, `make bi` build/start Superset và provision connection, 3 datasets, 3 charts và dashboard. Nếu khởi động trước dữ liệu, chạy `make dashboard` sau khi pipeline hoàn tất.

Dashboard: `/superset/dashboard/cosmetics-commerce/` gồm revenue theo category, ordered session funnel và phân bố RFM. Filter category/brand chỉ áp dụng cho sales chart; không áp dụng sai grain cho funnel/RFM.

Account dashboard trên ClickHouse chỉ có quyền đọc cosmetics_analytics. Credentials lấy từ .env.deploy; không dùng default/admin. Cache tắt để release mới xuất hiện khi refresh. Có thể thêm filter thời gian và charts qua UI; ghi nhận/export thay đổi trước khi chạy lại provisioner vì provisioner sẽ đồng bộ lại dashboard quản lý bởi code.

Dataset nguồn không có order_id/quantity/refund; không gắn nhãn AOV đơn hàng. Xem [data contract](data_contract.md).
