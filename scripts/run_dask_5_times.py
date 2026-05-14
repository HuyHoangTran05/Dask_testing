import subprocess
import time
import pandas as pd
import os

RESULT_PATH = "output/dask_benchmark_results.csv"

os.makedirs("output", exist_ok=True)

results = []

for i in range(1, 6):
    print(f"\n===== DASK RUN {i}/5 =====")

    start = time.perf_counter()

    subprocess.run(
        ["python", "scripts/benchmark_dask_taxi.py"],
        check=True
    )

    elapsed = time.perf_counter() - start

    results.append({
        "framework": "Dask",
        "run": i,
        "time_seconds": elapsed
    })

df = pd.DataFrame(results)
df.to_csv(RESULT_PATH, index=False)

print("\nKết quả benchmark:")
print(df)
print("\nAverage time:", df["time_seconds"].mean())
print(f"\nĐã lưu kết quả tại: {RESULT_PATH}")