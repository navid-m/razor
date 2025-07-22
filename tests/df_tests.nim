import
    unittest,
    javelin,
    times

test "Can create a new DF, retrieve head, describe, create and read a CSV":
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
