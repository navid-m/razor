import time
import pandas as pd

start = time.time()

df = pd.DataFrame({
    "id": range(10_000_000),
    "value": [i * 1.5 for i in range(10_000_000)]
})

print(f"Created in {time.time() - start:.3f} seconds")
