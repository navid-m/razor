import
    unittest,
    razor,
    tables,
    times

test "Create a new DF, retrieve head, describe, create and read a CSV":
    var df = newDataFrame()
    df["name"] = newSeries(@["Alice", "Bob", "Charlie"])
    df["age"] = newSeries(@[25'i64, 30'i64, 35'i64])
    df["score"] = newSeries(@[85.5, 92.0, 78.5])
    echo df.head(2)
    echo df.describe()
    echo df["age"].mean()
    df.toCsv("data.csv")
    discard readCsv("data.csv")
    discard dateRange(now(), 10, "D")

test "Index and slice DataFrame rows and columns":
    var df = newDataFrame()
    df["city"] = newSeries(@["NY", "SF", "LA", "CHI"])
    df["temp"] = newSeries(@[22.0, 19.5, 25.1, 17.8])
    assert(df["city"][1] == newValue("SF"), "Dataframe lookup")

test "Handle missing values with fillna and dropna":
    var df = newDataFrame()
    df["value"] = newSeries(@[1.0, NaN, 3.0, NaN, 5.0])
    let filled = df["value"].fillNa(v 0.0)
    check filled.toSeq() == @[v 1.0, v 0.0, v 3.0, v 0.0, v 5.0]
    let dropped = df["value"].dropNa()
    check dropped.toSeq() == @[v 1.0, v 3.0, v 5.0]

test "Perform arithmetic operations on Series":
    var df = newDataFrame()
    df["a"] = newSeries(@[1'i64, 2, 3])
    df["b"] = newSeries(@[10'i64, 20, 30])
    let c = df["a"] + df["b"]
    check c.toSeq() == @[11'i64, 22, 33]

test "Generate a correct date range":
    let start = parse("2024-01-01", "yyyy-MM-dd")
    let dr = dateRange(start, 5, "D")
    check dr.len == 5
    check dr[0] == v start
    check dr[4] == v start + 4.days

test "Group by a categorical column, then compute mean":
    var df = newDataFrame()
    df["dept"] = newSeries(@["HR", "IT", "HR", "IT"])
    df["salary"] = newSeries(@[50_000'i64, 60_000, 55_000, 65_000])
    let result = df.groupBy("dept").mean("salary")
    check result["HR"] == 52_500.0
    check result["IT"] == 62_500.0

test "Boolean filtering on Series":
    var df = newDataFrame()
    df["x"] = newSeries(@[1, 2, 3, 4])
    let mask = df["x"] > 2
    let filtered = df["x"].mask(mask)
    check filtered.toSeq() == @[v 3, v 4]

test "Sort dataframe by a numeric column ascending and descending":
    var df = newDataFrame()
    df["name"] = newSeries(@["Bob", "Alice", "Charlie"])
    df["score"] = newSeries(@[92.0, 85.5, 78.5])
    let sortedAsc = df.sort("score")
    check sortedAsc["name"].toSeq() == @[v "Charlie", v "Alice", v "Bob"]
    let sortedDesc = df.sort("score", ascending = false)
    check sortedDesc["name"].toSeq() == @[v "Bob", v "Alice", v "Charlie"]

test "Concatenate two dataframes vertically":
    var df1 = newDataFrame()
    df1["id"] = newSeries(@[1, 2])
    var df2 = newDataFrame()
    df2["id"] = newSeries(@[3, 4])
    let combined = concat(df1, df2)
    check combined["id"].toSeq() == @[v 1, v 2, v 3, v 4]

test "Rename columns in a dataframe":
    var df = newDataFrame()
    df["old_name"] = newSeries(@[1, 2, 3])
    df.renameColumn("old_name", "new_name")
    check df.columns.hasKey("new_name")
    check not df.columns.hasKey("old_name")

test "Set values conditionally using mask":
    var df = newDataFrame()
    df["score"] = newSeries(@[40.0, 55.0, 70.0])
    check df["score"].mask(@[true, false, true]).data == @[v 40.0, v 70.0]

test "Mathematical operations on columns":
    var df = newDataFrame()
    df["a"] = newSeries(@[10, 20, 30])
    df["b"] = newSeries(@[1, 2, 3])
    check (df["a"] - df["b"]).toSeq() == @[v 9, v 18, v 27]
    check (df["a"] / df["b"]).toSeq() == @[v 10.0, v 10.0, v 10.0]

test "Quantile and median":
    var s = newSeries(@[1, 2, 3, 4, 5])
    check s.quantile(0.5) == v 3.0
    check s.median() == v 3.0

test "Standard deviation and variance":
    var s = newSeries(@[1.0, 2.0, 3.0])
    check s.standardDeviation().round(3) == v 1.0
    check s.variance().round(3) == v 1.0

test "Drop duplicates and find unique values":
    var df = newDataFrame()
    df["name"] = newSeries(@["Alice", "Bob", "Alice"])
    let unique = df["name"].unique()
    check unique.toSeq() == @[v "Alice", v "Bob"]
    let dropped = df.dropDuplicates()
    check dropped["name"].toSeq() == @[v "Alice", v "Bob"]

test "Apply returning a new series":
    var df = newDataFrame()
    df["x"] = newSeries(@[1, 2, 3])
    df["y"] = newSeries(@[4, 5, 6])
    let z = df.applyRows(proc(row: OrderedTable[string, Value]): Value = v row[
            "x"].int * row["y"].int)
    check z.toSeq() == @[v 4, v 10, v 18]
