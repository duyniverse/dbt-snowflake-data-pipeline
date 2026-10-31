# Data Pipeline: dbt & Snowflake Orchestration with Apache Airflow (Cosmos)

Dự án xây dựng và tự động hóa hệ thống Data Pipeline, thực hiện chuyển đổi dữ liệu (Data Transformation) trên Snowflake bằng dbt Core, và điều phối luồng chạy (Orchestration) tự động thông qua Apache Airflow (triển khai qua Astronomer Cosmos).

## 📌 Tổng Quan Dự Án

Dự án được tách biệt rõ ràng giữa tầng Biến đổi dữ liệu (Data Transformation layer) và tầng Điều phối (Orchestration layer):

- **Data Transformation (`data_pipeline/`)**: Chứa toàn bộ dbt project bao gồm các models (staging, intermediate, marts), test cases, documentation và macros phục vụ biến đổi dữ liệu trực tiếp trên Snowflake Data Warehouse.
- **Orchestration (`dbt-dag/`)**: Sử dụng **Astronomer Cosmos** để tự động parse dbt project thành các Airflow DAG/Task nhóm chuyên biệt (Task Groups), giúp theo dõi và quản lý vòng đời chạy của từng model dbt trực quan trên UI của Airflow.

## 📊 Nguồn Dữ Liệu (Data Source)

Dự án sử dụng bộ dữ liệu mẫu chuẩn (Sample Dataset) được cung cấp sẵn trên Snowflake phục vụ cho bài toán phân tích & xử lý dữ liệu lớn (Big Data / Analytics):

* **Database**: `SNOWFLAKE_SAMPLE_DATA`
* **Schema**: `TPCH_SF1`
* **Tables**:
  * `ORDERS`: Khai báo thông tin chi tiết các đơn hàng.
  * `LINEITEM`: Chi tiết từng mục hàng (items) nằm trong mỗi đơn hàng.
 
## 🛠 Tech Stack

* **dbt**: Tầng biến đổi dữ liệu (Transformation), quản lý data models (Staging, Intermediate, Marts) và kiểm thử chất lượng dữ liệu (Data Quality Testing).
* **Snowflake**: Cloud Data Warehouse đóng vai trò lưu trữ và thực thi các câu truy vấn biến đổi dữ liệu với hiệu năng cao.
* **Apache Airflow**: Nền tảng điều phối (Orchestration) giúp tự động hóa và lập lịch cho toàn bộ Data Pipeline.
* **Astronomer Cosmos**: Thư viện kết nối dbt với Airflow, tự động parse các models dbt thành các Airflow Task Groups trực quan mà không cần build sẵn `manifest.json`.

## 🏗 Cấu Trúc Dự Án

```text
dbt-snowflake-project/
├── data_pipeline/             # 🏢 dbt Project (Data Transformation Layer)
│   ├── .vscode/               # Cấu hình môi trường phát triển trên VS Code
│   ├── analyses/              # Các câu truy vấn SQL phân tích ad-hoc
│   ├── dbt_packages/          # Các dbt packages được cài đặt từ hub (dbt_utils, etc.)
│   ├── logs/                  # Nhật ký thực thi dbt local
│   ├── macros/                # Custom SQL macros phục vụ tái sử dụng logic
│   ├── models/                # Các tầng data models (staging, intermediate, marts)
│   ├── seeds/                 # Dữ liệu tĩnh thô dạng CSV
│   ├── snapshots/             # Logic quản lý thay đổi dữ liệu theo thời gian (SCD Type 2)
│   ├── target/                # Thư mục compiled SQL & artifacts sau khi dbt run
│   ├── tests/                 # Custom data quality tests
│   ├── .gitignore             # Git ignore riêng cho dbt project
│   ├── dbt_project.yml        # File cấu hình chính của dbt project
│   ├── package-lock.yml       # Khóa phiên bản dbt packages chính xác
│   ├── packages.yml           # Khai báo các dbt packages phụ thuộc
│   ├── README.md              # Tài liệu riêng của dbt project
│   └── requirements.txt       # Python packages phục vụ chạy dbt local
│
├── dbt-dag/                   # ⚡ Airflow Project (Orchestration Layer via Astro CLI)
│   ├── .astro/                # Cấu hình nội bộ của Astronomer CLI
│   ├── dags/                  # Định nghĩa các Airflow DAGs & Cosmos Execution Config
│   ├── include/               # Chứa các file script, SQL hoặc tài nguyên dùng chung
│   ├── plugins/               # Custom Airflow plugins & hooks
│   ├── tests/                 # Unit tests cho Airflow DAGs
│   ├── .dockerignore          # Cấu hình loại trừ file khi build Docker image
│   ├── .env                   # Biến môi trường chạy local (Airflow / Astro)
│   ├── .gitignore             # Git ignore riêng cho Airflow project
│   ├── airflow_settings.yaml  # Cấu hình Connections, Variables tự động cho Astro
│   ├── Dockerfile             # Custom Docker Image & cài đặt Virtual environment
│   ├── packages.txt           # OS-level packages cài vào Docker container
│   ├── README.md              # Tài liệu hướng dẫn mặc định của Astro CLI
│   └── requirements.txt       # Python packages (astronomer-cosmos, Snowflake provider)
│
└── .gitignore                 # Git ignore tổng tại root repo
```
## 🚀 Step-by-Step Setup

### Bước 1: Cấu Hình Môi Trường Trên Snowflake (Snowflake Setup)

1. **Đăng ký tài khoản Snowflake:**
   * Khởi tạo tài khoản Snowflake mới (nếu chưa có).
   * Lưu lại thông tin **Username**, **Password** và **Account Locator** (dùng để cấu hình biến kết nối trong `profiles.yml`)
   * **Account Locator** có định dạng: `<locator>.<region>`
     * Mở một Worksheet mới trên Snowflake và chạy 2 câu lệnh SQL:
       ```sql
       SELECT CURRENT_ACCOUNT(); -- Lấy locator
       SELECT CURRENT_REGION();  -- Lấy region
       ```
     * Ghép nối 2 kết quả trả về theo đúng định dạng: `<locator>.<region>` (Ví dụ: `xy12345.ap-southeast-1`).

2. **Khởi tạo và phân quyền trên Snowflake Worksheets:**
   Đăng nhập vào Snowflake, mở một Worksheet mới và chạy chuỗi lệnh SQL bên dưới để dựng môi trường DWH, Database và phân quyền RBAC (Role-Based Access Control):

   ```sql
   -- 1. Chuyển sang quyền cao nhất để khởi tạo tài nguyên
   USE ROLE accountadmin;

   -- 2. Tạo Data Warehouse với kích thước nhỏ nhất (XSMALL) để tối ưu chi phí
   CREATE WAREHOUSE IF NOT EXISTS dbt_wh WITH WAREHOUSE_SIZE = 'XSMALL';

   -- 3. Tạo Database cho dự án
   CREATE DATABASE IF NOT EXISTS dbt_db;

   -- 4. Tạo một Role riêng cho dự án dbt
   CREATE ROLE IF NOT EXISTS dbt_role;

   -- 5. Kiểm tra quyền hiện tại trên Warehouse
   SHOW GRANTS ON WAREHOUSE dbt_wh;

   -- 6. Cấp quyền sử dụng dbt_wh cho dbt_role
   
   -- 7. Gán dbt_role cho user cá nhân (nlbduy)
   GRANT ROLE dbt_role TO USER nlbduy;

   -- 8. Cấp toàn bộ quyền đọc/ghi/khởi tạo trên database dbt_db cho dbt_role
   GRANT ALL ON DATABASE dbt_db TO ROLE dbt_role;

   ```
### Bước 2: Cấu Hình và Kiểm Thử dbt Local (dbt Setup)

*Lưu ý: Toàn bộ quá trình phát triển và thực thi bên dưới được thực hiện trên môi trường **Ubuntu (WSL2)** với **Python 3.11** trong Virtual Environment (`venv`) đã được khởi tạo sẵn.*

Các core packages được cài đặt bao gồm dbt-core (v1.11.4) và adapter dbt-snowflake. Lưu ý kiểm tra tính tương thích giữa phiên bản Python trong Virtual Environment (venv) và phiên bản các dbt packages để tránh xung đột thư viện trong quá trình thực thi.
Dùng command `dbt --version` để kiểm tra độ tương thích.

1. **Cài đặt các thư viện phụ thuộc:**
   Truy cập vào thư mục `data_pipeline` và cài đặt các packages từ file `requirements.txt`:
   ```bash
   cd data_pipeline
   pip install -r requirements.txt
2. **Khởi tạo dbt project**
   Tại terminal: gõ `dbt init`
   Hệ thống sẽ hiển thị các luồng hỏi đáp (Interactive Prompt) để tự động khởi tạo cấu hình kết nối:

- Select Adapter: Chọn Snowflake (nhập số tương ứng).

- Account: Nhập Account Locator đã lấy ở Bước 1 (Dạng `<locator>.<region>`, lấy tại menu Admin -> Account trên Snowflake UI hoặc dùng câu lệnh SQL).

- User / Password: Nhập thông tin tài khoản đăng nhập Snowflake.

- Role: Nhập `dbt_role` (Role đã phân quyền ở Bước 1).

- Warehouse: Nhập `dbt_wh`.

- Database: Nhập `dbt_db`.

- Schema: Nhập `dbt_shema`.

- Threads: Nhập 10 (Số lượng luồng xử lý song song - tùy chọn).

=> File cấu hình kết nối sau khi tạo sẽ được lưu duy nhất tại thư mục gốc của người dùng (`~/.dbt/profiles.yml`). Trong trường hợp nhập sai thông tin ở các bước trên, có thể mở trực tiếp file `profiles.yml` này để chỉnh sửa lại thông số.

3. Kiểm thử kết nối với `dbt debug`

### Bước 3: Triển Khai Logic Transfomation Với dbt (dbt Modeling)

*Toàn bộ cấu trúc logic kỹ thuật của dự án đã được thiết lập sẵn trong thư mục `data_pipeline/`.*

* **Cấu hình Nguồn Dữ liệu (Source Configuration):** Khai báo các bảng dữ liệu thô từ schema `TPCH_SF1` (`SNOWFLAKE_SAMPLE_DATA`).
* **Xây dựng Data Models:** Phát triển các tầng models chuyển đổi dữ liệu theo chuẩn phân lớp:
  * `staging`: Trích xuất, làm sạch và chuẩn hóa kiểu dữ liệu ban đầu.
  * `intermediate`: Thực hiện các phép JOIN, tính toán logic kinh doanh trung gian.
  * `marts`: Tổng hợp dữ liệu đầu ra hoàn chỉnh sẵn sàng phục vụ báo cáo/BI.
* **Cấu hình `dbt_project.yml` & Tests:** Quản lý materialization (view/table) cho từng tầng model và thiết lập các bộ kiểm thử tự động (generic & singular tests) đảm bảo tính toàn vẹn dữ liệu.

### Bước 4: Khởi Tạo Môi Trường Điều Phối Với Apache Airflow & Astronomer (Airflow Setup)

Sau khi hoàn tất phát triển dbt project, tiếp tục thiết lập Apache Airflow để quản lý và điều phối toàn bộ luồng chạy tự động.

1. **Cài đặt Docker Desktop:**
   * Tải và cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/) trên máy tính. 
   * Kích hoạt tích hợp Docker với **WSL2 (Ubuntu)** để làm môi trường chứa các containers vận hành Airflow.

2. **Cài đặt Astronomer CLI (Astro CLI):**
   * Astro CLI là công cụ dòng lệnh hệ thống (Global CLI tool) dùng để quản lý Airflow container, không phải Python package nên **không cài đặt qua `pip` trong `venv`**.
   * Mở Terminal trên Ubuntu và thực thi lệnh cài đặt hệ thống:
     ```bash
     curl -sSL [https://install.astronomer.io](https://install.astronomer.io) | sudo bash
     ```
   * Nhập mật khẩu Ubuntu khi được yêu cầu để hoàn tất cài đặt Global CLI.

3. **Khởi tạo cấu trúc Airflow Project (`dbt-dag`):**
   Di chuyển ra thư mục gốc để tạo project Airflow nằm **cùng cấp** với dbt project:
   ```bash
   # Quay trở lại thư mục gốc dự án
   cd ..

   # Tạo thư mục riêng cho Airflow Project
   mkdir dbt-dag
   cd dbt-dag

   # Khởi tạo khung dự án Airflow tiêu chuẩn
   astro dev init

### Bước 5: Cấu Hình Chi Tiết Airflow & Triển Khai Cosmos DAG (Airflow Configuration & DAG Deployment)

#### 1. Cấu hình Dockerfile (`dbt-dag/Dockerfile`)
- Dùng Image được thiết lập sẵn từ Astronomer: `astro-runtime:11.3.0`
-  Khởi tạo Virtual Environment riêng biệt (`dbt_venv`) ngay trong Docker Container của Airflow để cài đặt `dbt-snowflake`, giúp cô lập môi trường execution tránh xung đột phiên bản package với Airflow:

```dockerfile
FROM quay.io/astronomer/astro-runtime:11.3.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```

#### 2. Khai báo Dependencies (dbt-dag/requirements.txt)
Khai báo các thư viện phụ thuộc cần thiết cho Airflow Orchestration
```
astronomer-cosmos
apache-airflow-providers-snowflake
```
- `astronomer-cosmos`: Thư viện tự động parse dbt project thành các Airflow Task Groups/Nodes riêng biệt, hiển thị trực quan toàn bộ Dòng chảy dữ liệu (Data Lineage) trên giao diện Airflow UI.

- `apache-airflow-providers-snowflake`: Bộ Provider chính chủ từ Airflow để khởi tạo Connections, Hooks và quản lý credentials kết nối tới Snowflake.

#### 3. Khởi chạy Airflow Cluster với Astro CLI & Troubleshooting

Tại thư mục dbt-dag, gõ lệnh khởi chạy container: `astro dev start`

🛠 Các lỗi hạ tầng thường gặp & Cách khắc phục (Troubleshooting Guide):

❌ Lỗi: `The command 'docker' could not be found in this WSL 2 distro`

Nguyên nhân: Docker Desktop trên Windows chưa bật tính năng tích hợp với phân vùng WSL2 (Ubuntu).

Cách fix: Mở Docker Desktop -> Settings -> Resources -> WSL Integration -> Bật ON cho distro Ubuntu -> Chọn Apply & restart.

❌ Lỗi `socket / credential helper: UtilAcceptVsock:281: accept4 failed 110 / error getting credentials`

Nguyên nhân: Kẹt kết nối Credential Helper giữa WSL2 và Windows.

Cách fix:

Mở file C:\Users\<User>\.docker\config.json trên Windows, sửa `"credsStore": "desktop"` thành `"credsStore": ""` (hoặc xóa toàn bộ dòng `credsStore` và `Auth`).

Xóa config cache trong Linux: `rm -rf ~/.docker/config.json.`

Mở PowerShell (Admin) trên Windows gõ `wsl --shutdown` để reset socket WSL, sau đó khởi động lại Docker Desktop.

Khởi động lại VS Code và chạy lại lệnh `astro dev start`.

❌ Lỗi `Webserver Timeout: AttributeError: 'NoneType' object has no attribute 'permissions'`

Nguyên nhân: Phiên bản base image astro-runtime cũ bị xung đột phân quyền khởi tạo DB nội bộ với Python 3.11.

Cách fix: Cập nhật Dockerfile lên phiên bản ổn định `astro-runtime:11.3.0`, sau đó chạy `astro dev stop` và `astro dev restart` lại.

#### 4. Tích hợp dbt Project & Viết DAG (dbt-dag/dags/dbt-dag.py)

- Tạo thư mục dbt bên trong `dbt-dag/dags/` và sao chép thư mục `data_pipeline` vào `dbt-dag/dags/dbt/data_pipeline`.
- Tạo file định nghĩa DAG `dbt-dag/dags/dbt-dag.py`

```python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# 1. Cấu hình kết nối Snowflake qua Profile Mapping của Cosmos
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",  # Kết nối tới Airflow Connection ID đã tạo trên UI
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

# 2. Khởi tạo DbtDag tự động parse dbt models thành các Airflow Tasks
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/data_pipeline",
    ),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
```
> 💡 **Bản Chất Kiến Trúc & Cách Vận Hành Của Airflow + Cosmos:**
>
> 1. **Tại sao phải đặt folder dbt project vào bên trong thư mục `dags/`?**
>    * **Nguyên lý quét của Airflow:** Airflow chỉ tự động quét các tài nguyên nằm bên trong thư mục `dags/` để phát hiện và khởi tạo luồng công việc.
>    * **Đọc hiểu dự án tự động:** Nhờ có thư viện `astronomer-cosmos`, khi đặt dbt project (`data_pipeline/`) nằm bên trong `dags/dbt/`, Cosmos có thể trực tiếp truy cập, đọc hiểu toàn bộ các file SQL models và cấu hình dbt.
>    * **Trực quan hóa Lineage:** Thay vì chạy dbt như một "hộp đen" qua lệnh CLI đơn thuần, Cosmos tự động phân tách từng model SQL thành các Task Nodes trực quan tương ứng ngay trên giao diện Airflow UI để theo dõi dòng chảy dữ liệu.
>
> 2. **Vai trò của file định nghĩa DAG (`dbt-dag.py`):**
>    File Python này đóng vai trò là "cầu nối" (DAG Definition) để điều hướng và ra lệnh cho Airflow cách thức vận hành dbt project:
>    * **Trỏ đường dẫn dbt:** Chỉ định chính xác vị trí thư mục code dbt bên trong Container (`/usr/local/airflow/dags/dbt/data_pipeline`).
>    * **Chỉ định môi trường thực thi:** Khai báo đường dẫn thực thi `dbt` bên trong Virtual Environment (`dbt_venv`) đã cô lập ở Dockerfile để tránh xung đột thư viện với Airflow core.
>    * **Mapping kết nối Snowflake:** Lấy cấu hình Connection (`snowflake_conn`) từ Airflow UI để cấp quyền truy cập DWH cho dbt.
>    * **Tự động sinh Task:** Khởi tạo đối tượng `DbtDag` để Cosmos tự động sinh ra chuỗi các task execution (`dbt run`, `dbt test`) hoàn chỉnh.

#### 5. Cấu Hình Snowflake Connection Trên Airflow UI & Kích Hoạt Pipeline

1. **Truy cập Airflow Webserver:**
   * Sau khi lệnh `astro dev start` chạy thành công, Terminal sẽ trả về đường dẫn Web UI (mặc định: `http://localhost:8080`).
   * Đăng nhập với tài khoản mặc định:
     * **Username:** `admin`
     * **Password:** `admin`

2. **Cấu hình Kết nối Snowflake (Airflow Connection):**
   * Trên thanh menu, truy cập **Admin -> Connections** -> Nhấn nút **`+` (Add a new record)**.
   * Điền chi tiết các thông số kết nối:
     * **Connection Id:** `snowflake_conn` *(Phải khớp chính xác với `conn_id` đã khai báo trong file `dbt-dag.py`)*
     * **Connection Type:** `Snowflake`
     * **Account:** Account Locator dạng `<locator>.<region>` (Ví dụ: `xy12345.ap-southeast-1`)
     * **Login:** Username Snowflake
     * **Password:** Mật khẩu đăng nhập Snowflake
     * **Warehouse:** `dbt_wh`
     * **Database:** `dbt_db`
     * **Role:** `dbt_role`
     * **Insecure Mode:** Để trống
   * Nhấn **Save** để lưu lại.

   > 💡 **Bản chất của bước này:** Việc cấu hình tại **Admin -> Connections** chính là giao "chiếc chìa khóa" đăng nhập Snowflake an toàn cho Airflow. Khi Cosmos thực thi, nó sẽ tự động dùng Connection ID này để cung cấp credentials cho dbt kết nối tới Snowflake DWH mà không cần phải hardcode password trực tiếp trong mã nguồn code hay file cấu hình repository.

3. **Kích hoạt & Theo dõi Pipeline (Trigger & Monitor):**
   * Truy cập lại trang chủ **DAGs**, tìm DAG `dbt_dag`.
   * Gạt công tắc chuyển trạng thái từ **Off** sang **On (Unpause)**.
   * Nhấn nút **Trigger DAG** (biểu tượng Play ▶️) để tiến hành chạy luồng dữ liệu tự động.
   * Chuyển sang giao diện **Grid View** hoặc **Graph View** để theo dõi trạng thái thực thi từng task node (`dbt run`, `dbt test`) cũng như dòng chảy dữ liệu (Data Lineage) trực quan theo thời gian thực.
