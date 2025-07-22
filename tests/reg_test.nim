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
