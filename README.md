# Razor

Razor is a feature-rich, high-performance dataframe library, inspired by Python’s pandas but leveraging Nim’s static typing and speed.

## Features

-  **Dataframe Creation & I/O** - `toCsv`, `readCsv`
-  **Data Inspection** - `.head(n)`, `.describe()`, `.dtypes()`, indexing and slicing rows and columns
-  **Data Cleaning** - `fillNa`, `dropNa`, `dropDuplicates`, `replace`, `isin`
-  **Vectorized Operations** - `apply` and `applyRows` for custom logic
-  **Filtering & Sorting** - Boolean masking - `loc`, `mask`, chained filtering
-  **GroupBy & Aggregation** - `groupBy(...).mean(...)`, `sum(...)`, etc.
-  **Reshaping** - `melt` for pivoting wide to long format
-  **Time Series Utilities** - `dateRange`, `parse` for date handling

## Example

```nim
import razor

var df = newDataFrame()
df["name"] = newSeries(@["Alice", "Bob", "Charlie"])
df["age"] = newSeries(@[25, 30, 35])
df["score"] = newSeries(@[85.5, 92.0, 78.5])

echo df.head(2)
echo df.describe()
echo df["age"].mean()

df.toCsv("data.csv")
let df2 = readCsv("data.csv")
echo df2
```

Will output:

```
DataFrame (2x3):
             name          age        score
   0        Alice           25         85.5
   1          Bob           30         92.0

DataFrame (8x2):
              age        score
count            3            3
mean         30.0 85.33333333333333
 std          5.0 6.751543033509697
 min           25         78.5
 25%         27.5         82.0
 50%         30.0         85.5
 75%         32.5        88.75
 max           35         92.0

30.0
DataFrame (3x3):
             name          age        score
   0        Alice           25         85.5
   1          Bob           30         92.0
   2      Charlie           35         78.5
```

## Installation

```bash
nimble install razor
```

## Tests

All tests pass as of `2025-07`.

## Status

The library is production-ready for most standard data manipulation tasks.

## License

GPL-3.0 only.
