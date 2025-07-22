import
    unittest,
    razor,
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
