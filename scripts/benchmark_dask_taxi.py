import time
import os
import dask
import dask.dataframe as dd

DATA_PATH = "data/yellow_tripdata_2024-*.parquet"
OUTPUT_DIR = "output/dask_taxi_features"

os.makedirs("output", exist_ok=True)

def main():
    # Không dùng dask.distributed để tránh lỗi socket trên Windows/Python 3.13
    dask.config.set(scheduler="threads", num_workers=4)
    print("Dask scheduler: threads, num_workers=4")

    start = time.perf_counter()

    columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type"
    ]

    print("🔄 Đọc dữ liệu Parquet bằng Dask...")
    df = dd.read_parquet(DATA_PATH, columns=columns, engine="pyarrow")

    print("🔧 Làm sạch dữ liệu...")
    df["pickup_datetime"] = dd.to_datetime(df["tpep_pickup_datetime"])
    df["dropoff_datetime"] = dd.to_datetime(df["tpep_dropoff_datetime"])

    df["trip_duration_min"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60

    df = df[
        (df["trip_distance"] > 0) &
        (df["trip_distance"] < 100) &
        (df["fare_amount"] > 0) &
        (df["total_amount"] > 0) &
        (df["trip_duration_min"] > 0) &
        (df["trip_duration_min"] < 300)
    ]

    print("🧠 Tạo feature...")
    df["pickup_date"] = df["pickup_datetime"].dt.floor("D")
    df["pickup_hour"] = df["pickup_datetime"].dt.hour.astype("int16")
    df["PULocationID"] = df["PULocationID"].astype("int32")

    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"]
    df["tip_rate"] = df["tip_amount"] / df["total_amount"]

    print("📊 Groupby aggregation...")
    result = df.groupby(["pickup_date", "pickup_hour", "PULocationID"]).agg({
        "total_amount": ["count", "sum", "mean"],
        "trip_distance": "mean",
        "trip_duration_min": "mean",
        "fare_per_mile": "mean",
        "tip_rate": "mean"
    })

    result.columns = [
        "trip_count",
        "total_revenue",
        "avg_total_amount",
        "avg_trip_distance",
        "avg_trip_duration_min",
        "avg_fare_per_mile",
        "avg_tip_rate"
    ]

    result = result.reset_index()

    print("🔒 Ép kiểu dữ liệu trước khi ghi Parquet...")
    result["pickup_date"] = dd.to_datetime(result["pickup_date"])
    result["pickup_hour"] = result["pickup_hour"].astype("int16")
    result["PULocationID"] = result["PULocationID"].astype("int32")
    result["trip_count"] = result["trip_count"].astype("int64")

    float_cols = [
        "total_revenue",
        "avg_total_amount",
        "avg_trip_distance",
        "avg_trip_duration_min",
        "avg_fare_per_mile",
        "avg_tip_rate"
    ]

    for col in float_cols:
        result[col] = result[col].astype("float64")

    print("💾 Ghi kết quả ra Parquet...")
    result.to_parquet(
        OUTPUT_DIR,
        engine="pyarrow",
        write_index=False,
        overwrite=True,
        compute_kwargs={"scheduler": "threads", "num_workers": 4}
    )

    elapsed = time.perf_counter() - start

    print("=" * 60)
    print("✅ DASK BENCHMARK DONE")
    print(f"⏱️ Time: {elapsed:.2f} seconds")
    print(f"📁 Output: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()