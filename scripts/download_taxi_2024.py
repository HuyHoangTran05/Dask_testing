import os
import urllib.request

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = "data"

os.makedirs(DATA_DIR, exist_ok=True)

# Nếu máy yếu, để range(1, 4) trước: chỉ tải tháng 1-3
# Nếu muốn benchmark đầy đủ năm 2024, để range(1, 13)
for month in range(1, 13):
    filename = f"yellow_tripdata_2024-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    out_path = os.path.join(DATA_DIR, filename)

    if os.path.exists(out_path):
        print(f"✅ Đã có: {filename}")
        continue

    print(f"⬇️ Đang tải: {filename}")
    try:
        urllib.request.urlretrieve(url, out_path)
        print(f"✅ Xong: {filename}")
    except Exception as e:
        print(f"❌ Lỗi khi tải {filename}: {e}")

print("Hoàn tất tải dữ liệu.")