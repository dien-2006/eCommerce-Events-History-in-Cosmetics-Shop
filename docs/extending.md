# Thêm dự án và quản lý nhiều pipeline

`config/projects.yml` là registry. Mỗi entry sinh DAG `<project>_elt`, các namespace `<project>_bronze`, `<project>_silver`, `<project>_gold`, database `<project>_analytics`, prefix raw/artifacts và nhãn metrics riêng.

```yaml
projects:
  cosmetics:
    owner: data-platform
    schedule: "0 2 * * *"
    input_dir: /data/rawdata
    dbt_dir: /opt/project/dbt
    max_invalid_ratio: 0.01
  skincare:
    owner: retail-analytics
    schedule: "0 4 * * *"
    input_dir: /data/skincare
    dbt_dir: /opt/project/dbt
    max_invalid_ratio: 0.005
```

1. Thêm entry, dùng identifier lowercase chữ/số/underscore.
2. Thêm bind mount read-only `/data/skincare` cho các service Airflow trong Compose.
3. Cấp quyền đọc `<project>_analytics` cho tài khoản BI thích hợp; không tự mở quyền cho mọi database.
4. Rebuild image Airflow, restart scheduler/dag-processor/apiserver.
5. Kiểm tra import, trigger một run, kiểm tra registry và Grafana theo label project.
6. Superset provisioner hiện tạo dashboard cosmetics; clone dataset/charts cho project mới hoặc mở rộng provisioner có cấu hình.

Pool một slot dùng chung giúp giới hạn tải trên một host. Dữ liệu và metadata được tách namespace, nhưng tài khoản runtime vẫn thuộc cùng trusted platform: đây **không phải tenant isolation bảo mật**.

Một domain khác (ví dụ vận tải, y tế, giao dịch tài chính) cần contract, khóa dữ liệu, mô hình Gold, checks và schema serving riêng. Không thể chỉ đổi tên project rồi coi mọi nghiệp vụ có cùng funnel/RFM. Có thể tái sử dụng registry, cơ chế raw landing, DAG conventions và publication protocol; phải mở rộng interfaces/contract có kiểm thử cho domain mới.
