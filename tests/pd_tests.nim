import
    unittest,
    razor,
    tables

test "Join two dataframes on a common column":
    var df1 = newDataFrame()
    df1["id"] = newSeries(@[1, 2, 3])
    df1["name"] = newSeries(@["Alice", "Bob", "Charlie"])
    var df2 = newDataFrame()
    df2["id"] = newSeries(@[2, 3, 4])
    df2["score"] = newSeries(@[90.0, 80.0, 70.0])
    let merged = merge(df1, df2, on = "id")
    check merged["id"].toSeq() == @[v 2, v 3]
    check merged["name"].toSeq() == @[v "Bob", v "Charlie"]
    check merged["score"].toSeq() == @[v 90.0, v 80.0]

test "Pivot a dataframe (wide to long)":
    var df = newDataFrame()
    df["id"] = newSeries(@[1, 2])
    df["math"] = newSeries(@[90, 85])
    df["science"] = newSeries(@[80, 95])
    let melted = melt(df, idVars = @["id"], valueVars = @["math", "science"])
    check melted["id"].toSeq() == @[v 1, v 1, v 2, v 2]
    check melted["variable"].toSeq() == @[v "math", v "science", v "math", v "science"]
    check melted["value"].toSeq() == @[v 90, v 80, v 85, v 95]

test "Apply custom function to a Series":
    var s = newSeries(@[1, 2, 3])
    let squared = s.apply(proc(v: Value): Value = v(v.int) * v(v.int))
    check squared.toSeq() == @[v 1, v 4, v 9]

test "Apply custom function to each row":
    var df = newDataFrame()
    df["a"] = newSeries(@[1, 2])
    df["b"] = newSeries(@[3, 4])
    let summed = df.applyRows(proc(row: OrderedTable[string,
            Value]): Value = v (row["a"].int + row["b"].int))
    check summed.toSeq() == @[v 4, v 6]

test "Get dtypes of columns in a dataframe":
    var df = newDataFrame()
    df["id"] = newSeries(@[1, 2])
    df["name"] = newSeries(@["a", "b"])
    let types = df.dtypes()
    check types["id"] == "int"
    check types["name"] == "string"

test "Sort by multiple columns":
    var df = newDataFrame()
    df["group"] = newSeries(@["B", "A", "A", "B"])
    df["score"] = newSeries(@[2, 3, 1, 4])
    let sorted = df.sort(by = @["group", "score"])
    check sorted["score"].toSeq() == @[v 1, v 3, v 2, v 4]

test "Value-based filtering with multiple conditions":
    var df = newDataFrame()
    df["x"] = newSeries(@[1, 2, 3, 4])
    df["y"] = newSeries(@[10, 20, 30, 40])
    let filtered = df.loc((df["x"] > 2) & (df["y"] < 40))
    check filtered["x"].toSeq() == @[v 3]
    check filtered["y"].toSeq() == @[v 30]

test "Filter rows using isin":
    var df = newDataFrame()
    df["name"] = newSeries(@["Alice", "Bob", "Charlie"])
    let mask = df["name"].isin(@["Bob", "Charlie"])
    let result = df.loc(mask)
    check result["name"].toSeq() == @[v "Bob", v "Charlie"]
