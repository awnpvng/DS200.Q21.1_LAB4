# DS200.Q21.1 - Big Data Analysis (Lab 04)

![Java](https://img.shields.io/badge/Java-17-orange)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C)

## Thông tin sinh viên
- **MSSV:** 23520032
- **GitHub:** [awnpvng](https://github.com/awnpvng/DS200.Q21.1_LAB4.git)

## Cấu trúc thư mục
- `data/`: Chứa các file dữ liệu `.csv`.
- `main/src/`: Mã nguồn Java Spark (gồm các Task).
- `scripts/`: Chứa các script hỗ trợ chạy chương trình.
- `output/`: Thư mục chứa kết quả của các Task (.txt).
- `minh chung/`: Chứa các file hoặc hình ảnh báo cáo kết quả.

## Hướng dẫn cài đặt và chạy trên WSL (Ubuntu)

Dự án này sử dụng Java Spark và Maven. Khuyến nghị chạy trên môi trường Linux/WSL để dễ dàng cài đặt.

### 1. Cài đặt Java và Maven
Mở terminal WSL và chạy lệnh:
```bash
sudo apt update && sudo apt install default-jdk maven -y
```

### 2. Tải và cài đặt Apache Spark
Chạy lần lượt các lệnh sau trên WSL để tải Spark 3.5.0:
```bash
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
export PATH=$PATH:$(pwd)/spark-3.5.0-bin-hadoop3/bin
```

### 3. Thực thi chương trình
Từ thư mục gốc của dự án, chạy script sau để tự động build code và chạy tất cả các Task:
```bash
bash scripts/run.sh
```

Kết quả của các Task sẽ được lưu tự động thành các file `.txt` trong thư mục `output/`.

### 4. Kiểm tra kết quả
Để in toàn bộ kết quả ra terminal, bạn có thể chạy lệnh sau:
```bash
for file in output/*.txt; do echo -e "\n\033[1;33m--- KẾT QUẢ: $file ---\033[0m"; cat "$file"; done
```
