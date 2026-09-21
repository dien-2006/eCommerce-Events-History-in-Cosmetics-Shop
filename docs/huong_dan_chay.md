# Hướng dẫn chạy Cosmetics Commerce Lakehouse

Tài liệu này hướng dẫn chạy từ CSV đến dashboard Superset trên Linux hoặc Ubuntu trong WSL 2. Các lệnh Linux bên dưới chạy trong terminal tại thư mục gốc dự án; chỉ các khối ghi **PowerShell** mới chạy trên Windows.

Ngày cập nhật: **13/09/2026**. Spark/Iceberg và dbt đã qua kiểm thử local với dữ liệu mẫu. Toàn bộ stack Docker và 5 CSV nguồn chưa được nghiệm thu trên máy hiện tại. Xem [kết quả kiểm tra](validation.md).

## 1. Chuẩn bị máy

| Thành phần | Yêu cầu của dự án |
|---|---|
| Hệ điều hành | Linux amd64 hoặc WSL 2 chạy Linux containers |
| CPU | 4 vCPU |
| RAM cấp cho Linux/WSL | 16 GiB cho toàn bộ stack; core cần khoảng 12 GiB |
| Dung lượng trống | 50 GiB |
| Docker | Docker Engine hoạt động và Compose v2.39 trở lên |
| Công cụ host | Python 3.11/3.12, GNU Make |
| Mạng | Tải được Docker images, Python packages và Maven JARs |

Airflow, Spark, dbt và các database chạy trong container. Không cần cài Airflow vào Conda để chạy hệ thống bằng Docker.

### Nếu dùng Windows + WSL 2

1. Cài/mở [Docker Desktop cho Windows](https://docs.docker.com/desktop/setup/install/windows-install/).
2. Bật **Settings → General → Use the WSL 2 based engine**.
3. Vào **Settings → Resources → WSL Integration**, bật tích hợp cho Ubuntu đang chứa dự án, rồi áp dụng thay đổi. Tham khảo [hướng dẫn Docker WSL](https://docs.docker.com/desktop/features/wsl/).
4. Kiểm tra tài nguyên WSL. Lần kiểm tra gần nhất, môi trường này chỉ có 5,6 GiB RAM nên chưa đủ chạy core.

Nếu máy Windows còn đủ RAM cho cả Windows và WSL, mở file cấu hình từ **PowerShell**:

```powershell
notepad "$env:USERPROFILE\.wslconfig"
```

Thêm hoặc điều chỉnh phần `[wsl2]` hiện có, ví dụ:

```ini
[wsl2]
memory=16GB
processors=4
```

Lưu công việc trong các phiên WSL trước khi chạy lệnh sau ở **PowerShell**, vì lệnh sẽ dừng tất cả phiên WSL:

```powershell
wsl --shutdown
```

Mở lại Docker Desktop và Ubuntu. Cấu hình này giới hạn tài nguyên WSL, không tạo thêm RAM vật lý. Chi tiết: [cấu hình WSL của Microsoft](https://learn.microsoft.com/en-us/windows/wsl/wsl-config).

### Nếu dùng Linux trực tiếp

Cài Docker Engine và Compose plugin theo [hướng dẫn Docker Engine](https://docs.docker.com/engine/install/) cho bản phân phối của bạn. Tài khoản chạy dự án cần thực hiện được `docker info`.

### Cài Make và kiểm tra môi trường

Trên Ubuntu/WSL Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y make python3-venv
```

Đi vào thư mục dự án trên máy hiện tại:

```bash
cd "$HOME/projects/github/eCommerce Events History in Cosmetics Shop"
```

Nếu clone ở nơi khác, thay đường dẫn tương ứng. Kiểm tra:

```bash
python3 --version
make --version
docker version
docker compose version
docker info
free -h
df -h .
```

Chỉ tiếp tục khi Docker kết nối được server và tài nguyên đáp ứng bảng trên.

## 2. Tạo cấu hình và chuẩn bị dữ liệu

```bash
python3 scripts/bootstrap.py
python3 scripts/bootstrap.py --check
ls -lh data/rawdata/
```

Bootstrap tạo `.env.deploy` với mật khẩu ngẫu nhiên, tạo thư mục cần thiết và `dbt/profiles.yml` nếu chưa có. Nếu `.env.deploy` đã tồn tại, script giữ nguyên nội dung. Compose của phiên bản này dùng `.env.deploy`.

Mở `.env.deploy` trong editor để xem tài khoản đăng nhập và tùy chỉnh port khi cần. Không thay mật khẩu database tùy ý sau khi đã khởi tạo volumes: thay biến môi trường không tự đổi mật khẩu role đang lưu trong PostgreSQL.

Thư mục `data/rawdata/` trên workspace hiện có:

```text
2019-Oct.csv
2019-Nov.csv
2019-Dec.csv
2020-Jan.csv
2020-Feb.csv
```

Nếu chạy từ một bản clone chưa có dữ liệu, tự đặt CSV nguồn hoàn chỉnh vào thư mục này. Pipeline không tải hoặc tạo dữ liệu mẫu thay cho bạn. Header bắt buộc:

```csv
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
```

Cột `payment_method` là tùy chọn. Xem quy tắc dữ liệu trong [data contract](data_contract.md). Chỉ đưa file đã ghi xong vào thư mục nguồn.

## 3. Chạy hệ thống lần đầu

Chọn **một** trong hai cách dưới đây. Cách từng bước phù hợp khi cần quan sát và xử lý lỗi lần đầu.

### Cách A: Chạy từng bước

**Bước 1 — Khởi động core:**

```bash
make up
```

Lệnh kiểm tra cấu hình/tài nguyên, build images và chờ các dịch vụ core sẵn sàng. Lần build đầu cần tải nhiều dependency. Nếu lỗi hoặc hết thời gian chờ, xem mục xử lý lỗi trước khi tiếp tục.

**Bước 2 — Bật DAG và tạo lần chạy:**

```bash
docker compose --env-file .env.deploy exec -T airflow-scheduler airflow dags unpause cosmetics_elt
make trigger
```

Mở **http://localhost:8080**, đăng nhập bằng `AIRFLOW_ADMIN_USER` và `AIRFLOW_ADMIN_PASSWORD` trong `.env.deploy`. Chọn DAG `cosmetics_elt` và theo dõi lần chạy vừa tạo:

```text
land → bronze → transform → publish → finish
```

Các bước lần lượt lưu raw vào MinIO, tạo Bronze Iceberg, chạy dbt Silver/Gold và kiểm thử chất lượng, công bố sang ClickHouse, rồi ghi nhận hoàn tất. Chờ **cả 5 task success** trước khi bật dashboard.

Bật DAG cũng bật lịch chạy cấu hình sẵn, mặc định 02:00 UTC mỗi ngày. Nếu chỉ muốn demo một lần, sau khi run hoàn tất có thể tắt lịch:

```bash
docker compose --env-file .env.deploy exec -T airflow-scheduler airflow dags pause cosmetics_elt
```

**Bước 3 — Khởi động dashboard và giám sát:**

```bash
make bi
make monitoring
make status
```

`make monitoring` khởi động các dịch vụ giám sát nhưng không chờ health checks; nếu chúng đang khởi động, đợi rồi chạy lại `make status`.

### Cách B: Chạy tự động

Sau khi hoàn tất phần chuẩn bị:

```bash
make demo
make status
```

`make demo` tự bootstrap, kiểm tra môi trường, khởi động core, bật/trigger DAG, chờ pipeline thành công rồi bật BI và monitoring. Script chờ pipeline tối đa 4 giờ; đây là thời hạn của script, không phải thời gian xử lý được cam kết. Nó dùng toàn bộ CSV hiện có và để DAG ở trạng thái bật lịch.

## 4. Mở giao diện và xác nhận kết quả

| Giao diện | URL mặc định | Tài khoản trong `.env.deploy` |
|---|---|---|
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Superset | http://localhost:8088/superset/dashboard/cosmetics-commerce/ | `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` |
| Grafana | http://localhost:3000 | `admin` / `GRAFANA_ADMIN_PASSWORD` |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Spark UI | http://localhost:4040 | Không cấu hình đăng nhập |
| Prometheus | http://localhost:9090 | Không cấu hình đăng nhập |

Nếu thay port trong `.env.deploy`, dùng port tương ứng. Các URL localhost dành cho máy đang chạy stack; truy cập máy chủ từ xa xem [deployment](deployment.md#truy-cập-bên-ngoài-máy-chủ).

Một lần chạy có kết quả khi:

- Airflow hiển thị đủ 5 task `success`.
- `make status` không báo service thiếu/unhealthy hoặc DAG import errors; báo cáo pipeline có lần chạy `success`.
- Superset hiển thị dữ liệu ở các biểu đồ doanh thu theo danh mục, funnel phiên và phân nhóm RFM.

Nếu Superset đã chạy trước khi có dữ liệu, sau khi DAG thành công chạy:

```bash
make dashboard
```

Lệnh này đồng bộ dashboard do code quản lý; nếu đã chỉnh chart thủ công, lưu/export thay đổi trước khi chạy lại.

## 5. Chạy lại, xem log và dừng

### Nạp thêm dữ liệu hoặc chạy lại file cũ

Đặt CSV mới hoàn chỉnh vào `data/rawdata/`, bảo đảm DAG đã unpause rồi chạy:

```bash
make trigger
```

Pipeline được thiết kế nhận diện file bằng checksum và loại trùng sự kiện. Nếu task lỗi, xem log rồi dùng **Clear** task lỗi trong Airflow để retry cùng run. Không cần xóa database để chạy lại.

### Xem trạng thái và log

```bash
make status
docker compose --env-file .env.deploy ps --all
docker compose --env-file .env.deploy logs --tail 100 airflow-scheduler spark-thrift
```

Theo dõi log liên tục:

```bash
make logs
```

Nhấn `Ctrl+C` để thoát theo dõi log; các container vẫn chạy.

### Dừng và mở lại

Đợi DAG đang chạy hoàn tất rồi dừng:

```bash
make down
```

Lệnh giữ lại named volumes chứa dữ liệu. Không thêm `-v` nếu muốn giữ dữ liệu.

Mở lại:

```bash
make up
make bi
make monitoring
make status
```

Nếu sửa Python, DAG, SQL hoặc cấu hình được đóng gói trong image, cần build lại; `make up` và `make bi` đã có cờ `--build`.

## 6. Kiểm thử code khi chưa chạy Docker

Các lệnh này kiểm tra logic và model local, chưa xác nhận đường đi toàn hệ thống đến dashboard. Dùng Python 3.11/3.12:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt -c requirements.lock
python3 scripts/bootstrap.py
bash scripts/test.sh
.venv/bin/dbt parse --project-dir dbt --profiles-dir dbt
```

Kiểm thử Spark/Iceberg thật với dữ liệu nhỏ trên Ubuntu:

```bash
sudo apt-get install -y openjdk-17-jre-headless
java -version
bash scripts/integration.sh
```

Script tải dependencies, tạo Spark Thrift tạm, chạy scenario rồi dừng tài nguyên do nó tạo. Logs nằm trong `artifacts/integration/`. Hai test registry PostgreSQL sẽ skip nếu chưa cấu hình database kiểm thử riêng; không tính skip là pass.

## 7. Lỗi thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `docker could not be found in this WSL 2 distro` | Mở Docker Desktop và bật WSL Integration cho đúng Ubuntu |
| `Cannot connect to the Docker daemon` | Kiểm tra Docker Desktop/Engine đang chạy; chạy lại `docker info` |
| `make: command not found` | Cài `sudo apt-get install -y make` |
| Preflight báo thiếu RAM | Cấp đủ RAM cho WSL hoặc chuyển sang máy đủ tài nguyên; không bỏ qua preflight |
| Port đã được sử dụng | Sửa biến port tương ứng trong `.env.deploy`, ví dụ `AIRFLOW_PORT=18080`, rồi chạy lại `make up` |
| Build lỗi tải image/package/JAR | Kiểm tra kết nối tới registry/PyPI/Maven, đọc lỗi đầu tiên trong output rồi build lại |
| Container `unhealthy` hoặc startup timeout | Xem `ps --all` và `logs --tail 100 <tên-service>`; kiểm tra RAM, dung lượng và lỗi dịch vụ |
| DAG paused hoặc không chạy | Unpause `cosmetics_elt`; xem scheduler và DAG processor logs |
| `Bronze is empty` | Kiểm tra CSV thực sự có trong thư mục `RAW_DATA_DIR`, rồi kiểm tra task `land` và `bronze` |
| `transform` thất bại ở quality tests | Xem dbt logs/artifacts và dữ liệu quarantine; sửa dữ liệu theo contract trước khi retry |
| Superset chưa có chart/dữ liệu | Đợi pipeline `success`, chạy `make dashboard` nếu dashboard chưa được provision |
| `DagBag.__init__() ... include_examples` khi chạy local | Môi trường Conda đang dùng Airflow 3.3.1 khác bản 3.1.7 của dự án; dùng container đã ghim phiên bản để kiểm tra DAG |

Đọc thêm [troubleshooting](troubleshooting.md) cho lỗi dịch vụ và [operations](operations.md) cho replay, rollback, backup và retention.
