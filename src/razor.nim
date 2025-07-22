import
    tables,
    sequtils,
    strutils,
    algorithm,
    times,
    json,
    sets,
    os,
    razor/[models, values, ops]

proc newSeriesWithDataType*(
    data: seq[Value],
    name = "",
    dtype: DataType = dtString
): Series =
    ## Create a new series given a sequence of data, the name, and
    ## the datatype itself.
    var index = newSeq[string](data.len)
    for i in 0..<data.len:
        index[i] = $i
    Series(data: data, name: name, dtype: dtype, index: index)

proc newSeries*(data: seq[int64], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtInt
    )
proc newSeries*(data: seq[int], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtInt
    )
proc newSeries*(data: seq[float64], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtFloat
    )
proc newSeries*(data: seq[string], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtString
    )
proc newSeries*(data: seq[bool], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtBool
    )
proc newSeries*(data: seq[Value], name = ""): Series =
    var dtype = dtFloat
    for val in data:
        if not val.isNa():
            dtype = val.kind
            break
    result = newSeriesWithDataType(data, name, dtype)

proc len*(s: Series): int = s.data.len
proc `[]`*(s: Series, idx: int): Value = s.data[idx]
proc `[]`*(s: Series, idx: string): Value =
    let i = s.index.find(idx)
    if i == -1: raise newException(KeyError, "Key not found: " & idx)
    s.data[i]

proc `[]=`*(s: Series, idx: int, val: Value) = s.data[idx] = val

proc head*(s: Series, n = 5): Series =
    let endIdx = min(n, s.len)
    Series(data: s.data[0..<endIdx], name: s.name, dtype: s.dtype,
            index: s.index[0..<endIdx])

proc tail*(s: Series, n = 5): Series =
    let startIdx = max(0, s.len - n)
    Series(data: s.data[startIdx..^1], name: s.name, dtype: s.dtype,
            index: s.index[startIdx..^1])

proc sum*(s: Series): Value =
    if s.len == 0: return newValue(0'i64)
    result = s.data[0]
    for i in 1..<s.len:
        result = result + s.data[i]

proc mean*(s: Series): float64 =
    if s.len == 0: return 0.0
    let total = s.sum()
    case total.kind
    of dtInt: total.intVal.float64 / s.len.float64
    of dtFloat: total.floatVal / s.len.float64
    else: raise newException(ValueError, "Mean not supported for " & $total.kind)

proc max*(s: Series): Value =
    if s.len == 0: raise newException(ValueError, "Cannot find max of empty series")
    var maxVal = s.data[0]
    for val in s.data[1..^1]:
        if maxVal < val: maxVal = val
    maxVal

proc min*(s: Series): Value =
    if s.len == 0: raise newException(ValueError, "Cannot find min of empty series")
    var minVal = s.data[0]
    for val in s.data[1..^1]:
        if val < minVal: minVal = val
    minVal

proc sort*(s: Series, ascending = true): Series =
    var sortedData = s.data
    var sortedIndex = s.index
    let indices = toSeq(0..<s.len)
    var sortedIndices = indices

    sortedIndices.sort do (a, b: int) -> int:
        let cmp = if s.data[a] < s.data[b]: -1 elif s.data[a] == s.data[b]: 0 else: 1
        if ascending: cmp else: -cmp

    for i in 0..<s.len:
        sortedData[i] = s.data[sortedIndices[i]]
        sortedIndex[i] = s.index[sortedIndices[i]]

    Series(data: sortedData, name: s.name, dtype: s.dtype, index: sortedIndex)

proc unique*(s: Series): Series =
    var uniqueData: seq[Value] = @[]
    var uniqueIndex: seq[string] = @[]
    var seen: seq[Value] = @[]

    for i, val in s.data:
        if val notin seen:
            seen.add(val)
            uniqueData.add(val)
            uniqueIndex.add(s.index[i])

    Series(data: uniqueData, name: s.name, dtype: s.dtype, index: uniqueIndex)

proc newDataFrame*(): DataFrame
proc updateShape*(df: DataFrame)

proc valueCounts*(s: Series): DataFrame =
    var counts = initOrderedTable[string, int]()
    for val in s.data:
        let key = $val
        counts[key] = counts.getOrDefault(key, 0) + 1

    var values: seq[Value] = @[]
    var countVals: seq[Value] = @[]
    for key, count in counts:
        values.add(newValue(key))
        countVals.add(newValue(count.int64))

    result = newDataFrame()
    result.columns[s.name] = newSeriesWithDataType(values, s.name)
    result.columns["count"] = newSeriesWithDataType(countVals, "count")
    result.updateShape()

proc newDataFrame*(): DataFrame =
    DataFrame(
        columns: initOrderedTable[string, Series](),
        index: @[], shape: (0, 0)
    )

proc newDataFrame*(data: OrderedTable[string, Series]): DataFrame =
    result = DataFrame(columns: data, index: @[], shape: (0, 0))
    if data.len > 0:
        let firstSeries = toSeq(data.values)[0]
        result.index = newSeq[string](firstSeries.len)
        for i in 0..<firstSeries.len:
            result.index[i] = $i
    result.updateShape()

proc updateShape*(df: DataFrame) = df.shape = (df.index.len, df.columns.len)
proc len*(df: DataFrame): int = df.shape.rows

proc `[]`*(df: DataFrame, col: string): Series =
    if col notin df.columns:
        raise newException(KeyError, "Column not found: " & col)
    df.columns[col]

proc `[]=`*(df: DataFrame, col: string, series: Series) =
    df.columns[col] = series
    if df.index.len == 0 and series.len > 0:
        df.index = newSeq[string](series.len)
        for i in 0..<series.len:
            df.index[i] = $i
    df.updateShape()

proc head*(df: DataFrame, n = 5): DataFrame =
    let endIdx = min(n, df.len)
    result = newDataFrame()
    for name, series in df.columns:
        result[name] = series.head(endIdx)
    result.index = df.index[0..<endIdx]
    result.updateShape()

proc tail*(df: DataFrame, n = 5): DataFrame =
    let startIdx = max(0, df.len - n)
    result = newDataFrame()
    for name, series in df.columns:
        result[name] = series.tail(n)
    result.index = df.index[startIdx..^1]
    result.updateShape()

proc describe*(df: DataFrame): DataFrame =
    result = newDataFrame()
    var stats = @["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
    var statIndex = newSeq[string](stats.len)
    for i, stat in stats: statIndex[i] = stat

    for name, series in df.columns:
        if series.dtype in {dtInt, dtFloat}:
            var statValues: seq[Value] = @[]
            statValues.add(newValue(series.len.int64))
            statValues.add(newValue(series.mean()))
            statValues.add(newValue(0.0))
            statValues.add(series.min())
            statValues.add(newValue(0.0))
            statValues.add(newValue(0.0))
            statValues.add(newValue(0.0))
            statValues.add(series.max())

            result[name] = newSeriesWithDataType(statValues, name)

    result.index = statIndex
    result.updateShape()

proc renameColumn*(df: DataFrame, oldName, newName: string) =
    if not df.columns.hasKey(oldName):
        raise newException(ValueError, "No such column exists to rename: " & oldName)
    df.columns[newName] = df.columns[oldName]
    df.columns.del(oldName)

proc info*(df: DataFrame): string =
    result = "DataFrame Info:\n"
    result.add("Shape: " & $df.shape & "\n")
    result.add("Columns: " & $df.columns.len & "\n")
    for name, series in df.columns:
        result.add("  " & name & ": " & $series.dtype & " (" & $series.len & " values)\n")

proc concat*(dfl, dfr: DataFrame): DataFrame =
    ## Concatenate two dataframes vertically.
    ##
    ## Ensure both dataframes have the same columns and dtypes.
    for colName, series1 in dfl.columns:
        if not dfr.columns.hasKey(colName):
            raise newException(ValueError, "Column '" & colName & "' not found in second DataFrame.")
        let series2 = dfr.columns[colName]
        if series1.dtype != series2.dtype:
            raise newException(ValueError, "Column '" & colName & "' has mismatched dtypes.")

    var newDf = DataFrame()
    newDf.columns = initOrderedTable[string, Series]()
    newDf.index = dfl.index & dfr.index

    for colName, series1 in dfl.columns:
        let series2 = dfr.columns[colName]
        var newSeries = Series(
            name: colName,
            dtype: series1.dtype,
            data: series1.data & series2.data,
            index: dfl.index & dfr.index
        )
        newDf.columns[colName] = newSeries
    newDf.shape = (
        dfl.shape.rows + dfr.shape.rows,
        if dfl.shape.cols > 0: dfl.shape.cols else: dfr.shape.cols
    )
    return newDf


proc sort*(df: DataFrame, by: string, ascending = true): DataFrame =
    if by notin df.columns:
        raise newException(KeyError, "Column not found: " & by)

    let sortSeries = df.columns[by]
    let indices = toSeq(0..<df.len)
    var sortedIndices = indices

    sortedIndices.sort do (a, b: int) -> int:
        let cmp = if sortSeries.data[a] < sortSeries.data[b]: -1
                            elif sortSeries.data[a] == sortSeries.data[b]: 0
                            else: 1
        if ascending: cmp else: -cmp

    result = newDataFrame()
    for name, series in df.columns:
        var sortedData: seq[Value] = @[]
        var sortedIndex: seq[string] = @[]
        for i in sortedIndices:
            sortedData.add(series.data[i])
            sortedIndex.add(series.index[i])
        result[name] = Series(data: sortedData, name: name, dtype: series.dtype,
                index: sortedIndex)

    var newIndex: seq[string] = @[]
    for i in sortedIndices:
        newIndex.add(df.index[i])
    result.index = newIndex
    result.updateShape()

proc groupBy*(df: DataFrame, by: string): GroupedDataFrame =
    if by notin df.columns:
        raise newException(KeyError, "Column not found: " & by)

    var groups = initOrderedTable[string, seq[int]]()
    let groupSeries = df.columns[by]

    for i, val in groupSeries.data:
        let key = $val
        if key notin groups:
            groups[key] = @[]
        groups[key].add(i)

    var groupedDfs = initOrderedTable[string, DataFrame]()
    for key, indices in groups:
        var groupDf = newDataFrame()
        for name, series in df.columns:
            var groupData: seq[Value] = @[]
            var groupIndex: seq[string] = @[]
            for i in indices:
                groupData.add(series.data[i])
                groupIndex.add(series.index[i])
            groupDf[name] = Series(data: groupData, name: name,
                    dtype: series.dtype, index: groupIndex)

        var newIndex: seq[string] = @[]
        for i in indices:
            newIndex.add(df.index[i])
        groupDf.index = newIndex
        groupDf.updateShape()
        groupedDfs[key] = groupDf

    GroupedDataFrame(groups: groupedDfs, groupByColumn: by)

proc mean*(grouped: GroupedDataFrame, column: string): OrderedTable[string, float64] =
    ## Compute mean of a column for each group
    result = initOrderedTable[string, float64]()

    for groupKey, groupDf in grouped.groups:
        if column notin groupDf.columns:
            raise newException(KeyError, "Column '" & column & "' not found")

        let series = groupDf.columns[column]
        if series.dtype notin {dtInt, dtFloat}:
            raise newException(ValueError, "Mean can only be computed for numeric columns")

        result[groupKey] = series.mean()

proc sum*(grouped: GroupedDataFrame, column: string): OrderedTable[string, Value] =
    ## Compute sum of a column for each group
    result = initOrderedTable[string, Value]()

    for groupKey, groupDf in grouped.groups:
        if column notin groupDf.columns:
            raise newException(KeyError, "Column '" & column & "' not found")

        let series = groupDf.columns[column]
        result[groupKey] = series.sum()

proc count*(grouped: GroupedDataFrame): OrderedTable[string, int] =
    ## Count number of rows in each group
    result = initOrderedTable[string, int]()
    for groupKey, groupDf in grouped.groups:
        result[groupKey] = groupDf.len

proc min*(grouped: GroupedDataFrame, column: string): OrderedTable[string, Value] =
    ## Compute minimum of a column for each group
    result = initOrderedTable[string, Value]()

    for groupKey, groupDf in grouped.groups:
        if column notin groupDf.columns:
            raise newException(KeyError, "Column '" & column & "' not found")

        let series = groupDf.columns[column]
        result[groupKey] = series.min()

proc max*(grouped: GroupedDataFrame, column: string): OrderedTable[string, Value] =
    ## Compute maximum of a column for each group
    result = initOrderedTable[string, Value]()

    for groupKey, groupDf in grouped.groups:
        if column notin groupDf.columns:
            raise newException(KeyError, "Column '" & column & "' not found")

        let series = groupDf.columns[column]
        result[groupKey] = series.max()

proc agg*(
    grouped: GroupedDataFrame,
    column: string,
    operations: seq[string]
): DataFrame =
    ## Apply multiple aggregation operations to a column
    result = newDataFrame()

    let groupKeys = toSeq(grouped.groups.keys)
    result.index = groupKeys

    for op in operations:
        var values: seq[Value] = @[]

        case op.toLowerAscii()
        of "mean":
            let means = grouped.mean(column)
            for key in groupKeys:
                values.add(newValue(means[key]))
        of "sum":
            let sums = grouped.sum(column)
            for key in groupKeys:
                values.add(sums[key])
        of "count":
            let counts = grouped.count()
            for key in groupKeys:
                values.add(newValue(counts[key].int64))
        of "min":
            let mins = grouped.min(column)
            for key in groupKeys:
                values.add(mins[key])
        of "max":
            let maxs = grouped.max(column)
            for key in groupKeys:
                values.add(maxs[key])
        else:
            raise newException(
                ValueError,
                "Unsupported aggregation operation: " & op
            )

        result[column & "_" & op] = newSeriesWithDataType(values, column & "_" & op)

    result.updateShape()

proc `>`*(s: Series, value: Value): seq[bool] =
    ## Element-wise greater than comparison
    result = newSeq[bool](s.len)
    for i in 0..<s.len:
        result[i] = s.data[i] > value

proc `>`*[T](s: Series, value: T): seq[bool] =
    ## Element-wise greater than comparison with automatic conversion
    s > newValue(value)

proc `<`*(s: Series, value: Value): seq[bool] =
    ## Element-wise less than comparison
    result = newSeq[bool](s.len)
    for i in 0..<s.len:
        result[i] = s.data[i] < value

proc `<`*[T](s: Series, value: T): seq[bool] =
    ## Element-wise less than comparison with automatic conversion
    s < newValue(value)

proc `>=`*(s: Series, value: Value): seq[bool] =
    ## Element-wise greater than or equal comparison
    result = newSeq[bool](s.len)
    for i in 0..<s.len:
        result[i] = not (s.data[i] < value)

proc `>=`*[T](s: Series, value: T): seq[bool] =
    ## Element-wise greater than or equal comparison with automatic conversion
    s >= newValue(value)

proc `<=`*(s: Series, value: Value): seq[bool] =
    ## Element-wise less than or equal comparison
    result = newSeq[bool](s.len)
    for i in 0..<s.len:
        result[i] = not (value < s.data[i])

proc `<=`*[T](s: Series, value: T): seq[bool] =
    ## Element-wise less than or equal comparison with automatic conversion
    s <= newValue(value)

proc `==`*(s: Series, value: Value): seq[bool] =
    ## Element-wise equality comparison
    result = newSeq[bool](s.len)
    for i in 0..<s.len:
        result[i] = s.data[i] == value

proc `==`*[T](s: Series, value: T): seq[bool] =
    ## Element-wise equality comparison with automatic conversion
    s == newValue(value)

proc `!=`*(s: Series, value: Value): seq[bool] =
    ## Element-wise inequality comparison
    result = newSeq[bool](s.len)
    for i in 0..<s.len:
        result[i] = not (s.data[i] == value)

proc `!=`*[T](s: Series, value: T): seq[bool] =
    ## Element-wise inequality comparison with automatic conversion
    s != newValue(value)

proc mask*(s: Series, mask: seq[bool]): Series =
    ## Filter Series using a boolean mask, keeping only true values
    if s.len != mask.len:
        raise newException(ValueError, "Series and mask must have the same length")

    var filteredData: seq[Value] = @[]
    var filteredIndex: seq[string] = @[]

    for i in 0..<s.len:
        if mask[i]:
            filteredData.add(s.data[i])
            filteredIndex.add(s.index[i])

    Series(
        data: filteredData,
        name: s.name,
        dtype: s.dtype,
        index: filteredIndex
    )

proc loc*(s: Series, mask: seq[bool]): Series =
    s.mask(mask)

proc mask*(df: DataFrame, mask: seq[bool]): DataFrame =
    if df.len != mask.len:
        raise newException(ValueError, "DataFrame and mask must have the same length")

    result = newDataFrame()

    for name, series in df.columns:
        result[name] = series.mask(mask)

    var filteredIndex: seq[string] = @[]
    for i in 0..<df.len:
        if mask[i]:
            filteredIndex.add(df.index[i])

    result.index = filteredIndex
    result.updateShape()

proc loc*(df: DataFrame, mask: seq[bool]): DataFrame =
    df.mask(mask)

proc `>`*(a, b: Value): bool =
    if a.kind != b.kind: return false
    case a.kind
    of dtInt: a.intVal > b.intVal
    of dtFloat: a.floatVal > b.floatVal
    of dtString: a.stringVal > b.stringVal
    of dtBool: a.boolVal > b.boolVal
    of dtDateTime: a.dateTimeVal > b.dateTimeVal
    of dtNa: false

proc `>=`*(a, b: Value): bool =
    a > b or a == b

proc `<=`*(a, b: Value): bool =
    not (a > b)

proc toDateTime*(s: string, format = "yyyy-MM-dd"): DateTime =
    parse(s, format)

proc dateRange*(start: DateTime, periods: int, freq = "D"): Series =
    var dates: seq[Value] = @[]
    var current = start

    for i in 0..<periods:
        dates.add(newValue(current))
        case freq
        of "D": current = current + initDuration(days = 1)
        of "H": current = current + initDuration(hours = 1)
        of "M": current = current + initDuration(minutes = 1)
        else: current = current + initDuration(days = 1)

    newSeriesWithDataType(dates, "datetime")

proc resample*(df: DataFrame, rule: string, dateCol: string): DataFrame =
    # TODO: Handle resampling
    result = newDataFrame()
    result = df

proc readCsv*(filename: string, sep = ","): DataFrame =
    result = newDataFrame()
    if not fileExists(filename):
        raise newException(IOError, "File not found: " & filename)
    let content = readFile(filename)
    let lines = content.splitLines()

    if lines.len == 0:
        return result

    let headers = lines[0].split(sep)
    for header in headers:
        result[header.strip()] = newSeriesWithDataType(@[], header.strip())

    for i in 1..<lines.len:
        if lines[i].len == 0: continue
        let values = lines[i].split(sep)

        var j = 0
        for header in headers:
            if j < values.len:
                let val = values[j].strip()
                var parsedVal: Value
                try:
                    parsedVal = newValue(parseInt(val).int64)
                except ValueError:
                    try:
                        parsedVal = newValue(parseFloat(val))
                    except ValueError:
                        if val.toLowerAscii() in ["true", "false"]:
                            parsedVal = newValue(val.toLowerAscii() == "true")
                        else:
                            parsedVal = newValue(val)

                result[header.strip()].data.add(parsedVal)
            j += 1

    if result.columns.len > 0:
        let firstCol = toSeq(result.columns.values)[0]
        result.index = newSeq[string](firstCol.len)
        for i in 0..<firstCol.len:
            result.index[i] = $i
    result.updateShape()

proc toCsv*(df: DataFrame, filename: string, sep = ",", index = false) =
    ## Create a CSV file given the dataframe and filename.
    ##
    ## Optionally the delimiter and whether or not to utilize index.
    var content = ""
    let headers = toSeq(df.columns.keys)
    if index:
        content.add("index" & sep)
    content.add(headers.join(sep) & "\n")
    for i in 0..<df.len:
        if index:
            content.add(df.index[i] & sep)

        var row: seq[string] = @[]
        for header in headers:
            row.add($df.columns[header].data[i])

        content.add(row.join(sep) & "\n")

    writeFile(filename, content)

proc toJson*(df: DataFrame): JsonNode =
    result = newJObject()

    for name, series in df.columns:
        var jsonArray = newJArray()
        for val in series.data:
            case val.kind
            of dtInt: jsonArray.add(newJInt(val.intVal))
            of dtFloat: jsonArray.add(newJFloat(val.floatVal))
            of dtString: jsonArray.add(newJString(val.stringVal))
            of dtBool: jsonArray.add(newJBool(val.boolVal))
            of dtDateTime: jsonArray.add(newJString($val.dateTimeVal))
            of dtNa: discard
        result[name] = jsonArray

proc toParquet*(df: DataFrame, filename: string) =
    ## TODO: Add real parquet output.
    let jsonData = df.toJson()
    writeFile(filename.replace(".parquet", ".json"), $jsonData)
    echo "Note: Simplified export to JSON format (Parquet support requires external library)"

proc `$`*(s: Series): string =
    result = "Series: " & s.name & " (dtype: " & $s.dtype & ")\n"
    let displayCount = min(10, s.len)
    for i in 0..<displayCount:
        result.add(s.index[i] & "    " & $s.data[i] & "\n")
    if s.len > displayCount:
        result.add("... (" & $(s.len - displayCount) & " more)\n")
    result.add("Length: " & $s.len)

proc `$`*(df: DataFrame): string =
    result = "DataFrame (" & $df.shape.rows & "x" & $df.shape.cols & "):\n"
    let headers = toSeq(df.columns.keys)
    let displayRows = min(10, df.len)
    result.add("     ")
    for header in headers:
        result.add(header.align(12) & " ")
    result.add("\n")
    for i in 0..<displayRows:
        result.add(df.index[i].align(4) & " ")
        for header in headers:
            result.add(($df.columns[header].data[i]).align(12) & " ")
        result.add("\n")

    if df.len > displayRows:
        result.add("... (" & $(df.len - displayRows) & " more rows)\n")

proc slice*(
    df: DataFrame,
    startRow: int = 0,
    endRow: int = -1,
    startCol: int = 0,
    endCol: int = -1
): DataFrame =
    ## Slice the dataframe by rows and columns.
    ##
    ## Parameters:
    ##  - startRow: Starting row index (inclusive, default 0)
    ##  - endRow: Ending row index (exclusive, -1 means all rows)
    ##  - startCol: Starting column index (inclusive, default 0)
    ##  - endCol: Ending column index (exclusive, -1 means all columns)
    result = DataFrame()
    result.columns = initOrderedTable[string, Series]()

    let actualEndRow = if endRow == -1: df.shape.rows else: min(endRow, df.shape.rows)
    let actualEndCol = if endCol == -1: df.shape.cols else: min(endCol, df.shape.cols)

    if startRow < 0 or startRow >= df.shape.rows:
        raise newException(IndexDefect, "Start row index out of bounds")
    if actualEndRow < startRow:
        raise newException(IndexDefect, "End row must be greater than start row")
    if startCol < 0 or startCol >= df.shape.cols:
        raise newException(IndexDefect, "Start column index out of bounds")
    if actualEndCol < startCol:
        raise newException(IndexDefect, "End column must be greater than start column")

    let colNames = toSeq(df.columns.keys())
    let selectedCols = colNames[startCol..<actualEndCol]

    result.index = df.index[startRow..<actualEndRow]

    for colName in selectedCols:
        let originalSeries = df.columns[colName]
        let newSeries = Series()
        newSeries.name = originalSeries.name
        newSeries.dtype = originalSeries.dtype
        newSeries.data = originalSeries.data[startRow..<actualEndRow]
        newSeries.index = result.index
        result.columns[colName] = newSeries

    result.shape = (actualEndRow - startRow, actualEndCol - startCol)

proc slice*(df: DataFrame, rowSlice: Slice[int]): DataFrame =
    ## Slice the DataFrame by row range.
    result = df.slice(rowSlice.a, rowSlice.b + 1, 0, -1)

proc slice*(df: DataFrame, colNames: seq[string]): DataFrame =
    ## Slice the DataFrame by column names.
    ## Returns a new DataFrame with only the specified columns.
    result = DataFrame()
    result.columns = initOrderedTable[string, Series]()
    result.index = df.index

    for colName in colNames:
        if colName notin df.columns:
            raise newException(KeyError, "Column '" & colName & "' not found in DataFrame")
        result.columns[colName] = df.columns[colName]

    result.shape = (df.shape.rows, colNames.len)

proc toSeq*(series: Series): seq[Value] = result = series.data

proc isNa*(v: Value): bool =
    ## Check if a value is considered missing/null.
    case v.kind
    of dtString: v.stringVal == "" or v.stringVal == "NaN" or v.stringVal == "null"
    of dtFloat: v.floatVal != v.floatVal
    else: false

proc fillNa*(s: Series, fillValue: Value): Series =
    ## Fill missing values in a Series with the specified fill value.
    result = Series(
        name: s.name,
        dtype: s.dtype,
        index: s.index,
        data: newSeq[Value](s.len)
    )

    for i in 0..<s.len:
        if s.data[i].isNa():
            result.data[i] = fillValue
        else:
            result.data[i] = s.data[i]

proc fillNa*(df: DataFrame, fillValue: Value): DataFrame =
    ## Fill missing values in all columns of a DataFrame with the specified fill value
    result = newDataFrame()
    result.index = df.index

    for name, series in df.columns:
        result[name] = series.fillNa(fillValue)

    result.updateShape()

proc fillNa*(
    df: DataFrame,
    fillValues: OrderedTable[string, Value]
): DataFrame =
    ## Fill missing values in specific columns with different fill values
    result = newDataFrame()
    result.index = df.index

    for name, series in df.columns:
        if name in fillValues:
            result[name] = series.fillNa(fillValues[name])
        else:
            result[name] = series

    result.updateShape()

proc dropNa*(s: Series): Series =
    ## Remove missing values from a Series.
    var validData: seq[Value] = @[]
    var validIndex: seq[string] = @[]

    for i in 0..<s.len:
        if not s.data[i].isNa():
            validData.add(s.data[i])
            validIndex.add(s.index[i])

    Series(
        data: validData,
        name: s.name,
        dtype: s.dtype,
        index: validIndex
    )

proc dropNa*(df: DataFrame, how = "any"): DataFrame =
    ## Remove rows with missing values from a DataFrame.
    result = newDataFrame()
    var validRows: seq[int] = @[]

    for i in 0..<df.len:
        var naCount = 0
        var totalCols = df.columns.len

        for _, series in df.columns:
            if series.data[i].isNa():
                naCount += 1

        let shouldKeep = case how
        of "any": naCount == 0
        of "all": naCount < totalCols
        else: naCount == 0

        if shouldKeep:
            validRows.add(i)

    for name, series in df.columns:
        var cleanData: seq[Value] = @[]
        var cleanIndex: seq[string] = @[]

        for i in validRows:
            cleanData.add(series.data[i])
            cleanIndex.add(series.index[i])

        result[name] = Series(
            data: cleanData,
            name: name,
            dtype: series.dtype,
            index: cleanIndex
        )

    var newIndex: seq[string] = @[]
    for i in validRows:
        newIndex.add(df.index[i])
    result.index = newIndex
    result.updateShape()

proc quantile*(s: Series, q: float64): Value =
    if s.len == 0:
        raise newException(ValueError, "Cannot find quantile of empty series")
    if q < 0.0 or q > 1.0:
        raise newException(ValueError, "Quantile must be between 0.0 and 1.0")

    var numericValues: seq[float64] = @[]
    for val in s.data:
        case val.kind
        of dtInt: numericValues.add(val.intVal.float64)
        of dtFloat: numericValues.add(val.floatVal)
        else: discard

    if numericValues.len == 0:
        raise newException(ValueError, "No numeric values found for quantile calculation")

    numericValues.sort()

    if q == 0.0:
        return newValue(numericValues[0])
    if q == 1.0:
        return newValue(numericValues[^1])

    let
        index = q * (numericValues.len - 1).float64
        lower = int(index)
        upper = min(lower + 1, numericValues.len - 1)
        weight = index - lower.float64
        res = numericValues[lower] * (1.0 - weight) + numericValues[upper] * weight

    return newValue(res)

proc median*(s: Series): Value =
    ## Calculate median of a series.
    s.quantile(0.5)

proc quantile*(
    grouped: GroupedDataFrame,
    column: string,
    q: float64
): OrderedTable[string, Value] =
    ## Compute quantile of a column for each group.
    result = initOrderedTable[string, Value]()
    for groupKey, groupDf in grouped.groups:
        if column notin groupDf.columns:
            raise newException(KeyError, "Column '" & column & "' not found")
        let series = groupDf.columns[column]
        if series.dtype notin {dtInt, dtFloat}:
            raise newException(ValueError, "Quantile can only be computed for numeric columns")
        result[groupKey] = series.quantile(q)

proc median*(grouped: GroupedDataFrame, column: string): OrderedTable[string, Value] =
    ## Compute median of a column for each group.
    grouped.quantile(column, 0.5)

proc dropColumn*(df: DataFrame, columnName: string): DataFrame =
    if columnName notin df.columns:
        raise newException(KeyError, "Column not found: " & columnName)
    result = newDataFrame()
    result.index = df.index
    for name, series in df.columns:
        if name != columnName:
            result[name] = series
    result.updateShape()

proc replace*(s: Series, mask: seq[bool], replaceValue: Value): Series =
    if s.len != mask.len:
        raise newException(ValueError, "Series and mask must have the same length")

    result = Series(
        name: s.name,
        dtype: s.dtype,
        index: s.index,
        data: newSeq[Value](s.len)
    )

    for i in 0..<s.len:
        if mask[i]:
            result.data[i] = replaceValue
        else:
            result.data[i] = s.data[i]

proc merge*(df1, df2: DataFrame, on: string, how = "inner"): DataFrame =
    ## Merge two dataframes on a common column
    if on notin df1.columns or on notin df2.columns:
        raise newException(KeyError, "Column '" & on & "' not found in one or both DataFrames")

    result = newDataFrame()
    var mergedRows: seq[tuple[idx1, idx2: int]] = @[]
    for i1 in 0..<df1.len:
        for i2 in 0..<df2.len:
            if df1[on].data[i1] == df2[on].data[i2]:
                mergedRows.add((i1, i2))

    if mergedRows.len == 0:
        return result

    for colName, series in df1.columns:
        var newData: seq[Value] = @[]
        var newIndex: seq[string] = @[]
        for pair in mergedRows:
            newData.add(series.data[pair.idx1])
            newIndex.add($newData.len)
        result[colName] = Series(data: newData, name: colName,
                dtype: series.dtype, index: newIndex)

    for colName, series in df2.columns:
        if colName != on:
            var newData: seq[Value] = @[]
            var newIndex: seq[string] = @[]
            for pair in mergedRows:
                newData.add(series.data[pair.idx2])
                newIndex.add($newData.len)
            result[colName] = Series(data: newData, name: colName,
                    dtype: series.dtype, index: newIndex)

    result.index = newSeq[string](mergedRows.len)
    for i in 0..<mergedRows.len:
        result.index[i] = $i

    result.updateShape()

proc melt*(df: DataFrame, idVars: seq[string], valueVars: seq[string] = @[]): DataFrame =
    ## Pivot a dataframe from wide to long format.
    result = newDataFrame()

    let actualValueVars = if valueVars.len == 0:
        var vars: seq[string] = @[]
        for colName in df.columns.keys():
            if colName notin idVars:
                vars.add(colName)
        vars
    else:
        valueVars

    var
        idData: seq[seq[Value]] = @[]
        variableData: seq[Value] = @[]
        valueData: seq[Value] = @[]

    for idVar in idVars:
        idData.add(@[])

    for rowIdx in 0..<df.len:
        for valueVar in actualValueVars:
            for i, idVar in idVars:
                idData[i].add(df[idVar].data[rowIdx])

            variableData.add(newValue(valueVar))
            valueData.add(df[valueVar].data[rowIdx])

    for i, idVar in idVars:
        var newIndex: seq[string] = @[]
        for j in 0..<idData[i].len:
            newIndex.add($j)
        result[idVar] = Series(data: idData[i], name: idVar, dtype: df[
                idVar].dtype, index: newIndex)

    var newIndex: seq[string] = @[]
    for j in 0..<variableData.len:
        newIndex.add($j)

    result["variable"] = Series(data: variableData, name: "variable",
            dtype: dtString, index: newIndex)
    result["value"] = Series(data: valueData, name: "value", dtype: dtString,
            index: newIndex)

    result.index = newIndex
    result.updateShape()

proc apply*(s: Series, fn: proc(v: Value): Value): Series =
    ## Apply a function to each element in a Series
    var newData: seq[Value] = @[]
    for val in s.data:
        newData.add(fn(val))

    Series(
        data: newData,
        name: s.name,
        dtype: s.dtype,
        index: s.index
    )

proc applyRows*(
    df: DataFrame,
    fn: proc(row: OrderedTable[string, Value]): Value
): Series =
    ## Apply a function to each row of a DataFrame
    var results: seq[Value] = @[]
    var newIndex: seq[string] = @[]

    for rowIdx in 0..<df.len:
        var row = initOrderedTable[string, Value]()
        for colName, series in df.columns:
            row[colName] = series.data[rowIdx]

        results.add(fn(row))
        newIndex.add($rowIdx)

    Series(
        data: results,
        name: "applied",
        dtype: dtString,
        index: newIndex
    )

proc fillNa*(df: DataFrame, methoda: string): DataFrame =
    ## Fill missing values using specified method
    result = newDataFrame()
    result.index = df.index

    for name, series in df.columns:
        case methoda
        of "ffill":
            var newData = series.data
            var lastValidValue: Value
            var hasValidValue = false

            for i in 0..<newData.len:
                if not newData[i].isNa():
                    lastValidValue = newData[i]
                    hasValidValue = true
                elif hasValidValue and newData[i].isNa():
                    newData[i] = lastValidValue

            result[name] = Series(data: newData, name: name,
                    dtype: series.dtype, index: series.index)
        else:
            result[name] = series

    result.updateShape()

proc rollingMean*(s: Series, window: int): Series =
    ## Calculate rolling mean with specified window size
    var results: seq[Value] = @[]

    for i in 0..<s.len:
        if i < window - 1:
            results.add(newValue(NaN))
        else:
            var sum = 0.0
            var count = 0

            for j in (i - window + 1)..i:
                let val = s.data[j]
                case val.kind
                of dtInt:
                    sum += val.intVal.float64
                    count += 1
                of dtFloat:
                    sum += val.floatVal
                    count += 1
                else:
                    discard

            if count > 0:
                results.add(newValue(sum / count.float64))
            else:
                results.add(newValue(NaN))

    Series(
        data: results,
        name: s.name,
        dtype: dtFloat,
        index: s.index
    )

proc dtypes*(df: DataFrame): OrderedTable[string, string] =
    ## Get data types of all columns
    result = initOrderedTable[string, string]()
    for name, series in df.columns:
        case series.dtype
        of dtInt: result[name] = "int"
        of dtFloat: result[name] = "float"
        of dtString: result[name] = "string"
        of dtBool: result[name] = "bool"
        of dtDateTime: result[name] = "datetime"
        of dtNa: result[name] = "na"

proc sort*(df: DataFrame, by: seq[string], ascending: seq[bool] = @[]): DataFrame =
    ## Sort DataFrame by multiple columns
    if by.len == 0:
        raise newException(ValueError, "At least one column must be specified for sorting")

    for col in by:
        if col notin df.columns:
            raise newException(KeyError, "Column not found: " & col)

    let actualAscending = if ascending.len == 0:
        newSeq[bool](by.len).mapIt(true)
    else:
        ascending

    let indices = toSeq(0..<df.len)
    var sortedIndices = indices

    sortedIndices.sort do (a, b: int) -> int:
        for i, col in by:
            let valA = df.columns[col].data[a]
            let valB = df.columns[col].data[b]
            let cmp = if valA < valB: -1 elif valA == valB: 0 else: 1
            let finalCmp = if actualAscending[i]: cmp else: -cmp
            if finalCmp != 0:
                return finalCmp
        return 0

    result = newDataFrame()
    for name, series in df.columns:
        var sortedData: seq[Value] = @[]
        var sortedIndex: seq[string] = @[]
        for i in sortedIndices:
            sortedData.add(series.data[i])
            sortedIndex.add(series.index[i])
        result[name] = Series(data: sortedData, name: name, dtype: series.dtype,
                index: sortedIndex)

    var newIndex: seq[string] = @[]
    for i in sortedIndices:
        newIndex.add(df.index[i])
    result.index = newIndex
    result.updateShape()

proc `&`*(a, b: seq[bool]): seq[bool] =
    ## Element-wise AND operation for boolean sequences
    if a.len != b.len:
        raise newException(ValueError, "Sequences must have the same length")

    result = newSeq[bool](a.len)
    for i in 0..<a.len:
        result[i] = a[i] and b[i]

proc `|`*(a, b: seq[bool]): seq[bool] =
    ## Element-wise OR operation for boolean sequences
    if a.len != b.len:
        raise newException(ValueError, "Sequences must have the same length")

    result = newSeq[bool](a.len)
    for i in 0..<a.len:
        result[i] = a[i] or b[i]

proc isin*(s: Series, values: seq[string]): seq[bool] =
    ## Check if Series values are in the given sequence
    result = newSeq[bool](s.len)
    let valueSet = values.toHashSet()

    for i in 0..<s.len:
        let strVal = $s.data[i]
        result[i] = strVal in valueSet

proc isin*[T](s: Series, values: seq[T]): seq[bool] =
    ## Check if Series values are in the given sequence
    result = newSeq[bool](s.len)

    for i in 0..<s.len:
        result[i] = false
        for val in values:
            if s.data[i] == newValue(val):
                result[i] = true
                break

proc int*(v: Value): int64 =
    case v.kind
    of dtInt: v.intVal
    else: raise newException(ValueError, "Value is not an integer")

export
    DataType,
    Value,
    Series,
    DataFrame,
    newSeriesWithDataType,
    newSeries,
    newValue,
    newDataFrame,
    head,
    tail,
    sum,
    mean,
    max,
    min,
    sort,
    unique,
    valueCounts,
    describe,
    info,
    mask,
    dropna,
    groupBy,
    concat,
    dateRange,
    resample,
    na,
    readCsv,
    v,
    fillNa,
    dropNa,
    toCsv,
    toJson,
    toParquet,
    renameColumn,
    `$`,
    `==`,
    `<`,
    `+`,
    `-`,
    `*`,
    `/`
