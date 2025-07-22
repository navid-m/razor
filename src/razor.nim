import
    tables,
    sequtils,
    strutils,
    algorithm,
    times,
    json,
    os

type
    DataType* = enum
        dtInt,
        dtFloat,
        dtString,
        dtBool,
        dtDateTime

    Value* = object
        case kind*: DataType
        of dtInt: intVal*: int64
        of dtFloat: floatVal*: float64
        of dtString: stringVal*: string
        of dtBool: boolVal*: bool
        of dtDateTime: dateTimeVal*: DateTime

    Series* = ref object
        data*: seq[Value]
        name*: string
        dtype*: DataType
        index*: seq[string]

    DataFrame* = ref object
        columns*: OrderedTable[string, Series]
        index*: seq[string]
        shape*: tuple[rows, cols: int]

proc newValue*(val: int64): Value = Value(kind: dtInt, intVal: val)
proc newValue*(val: float64): Value = Value(kind: dtFloat, floatVal: val)
proc newValue*(val: string): Value = Value(kind: dtString, stringVal: val)
proc newValue*(val: bool): Value = Value(kind: dtBool, boolVal: val)
proc newValue*(val: DateTime): Value = Value(kind: dtDateTime, dateTimeVal: val)

proc `$`*(v: Value): string =
    case v.kind
    of dtInt: $v.intVal
    of dtFloat: $v.floatVal
    of dtString: v.stringVal
    of dtBool: $v.boolVal
    of dtDateTime: $v.dateTimeVal

proc `==`*(a, b: Value): bool =
    if a.kind != b.kind: return false
    case a.kind
    of dtInt: a.intVal == b.intVal
    of dtFloat: abs(a.floatVal - b.floatVal) < 1e-10
    of dtString: a.stringVal == b.stringVal
    of dtBool: a.boolVal == b.boolVal
    of dtDateTime: a.dateTimeVal == b.dateTimeVal

proc `<`*(a, b: Value): bool =
    if a.kind != b.kind: return false
    case a.kind
    of dtInt: a.intVal < b.intVal
    of dtFloat: a.floatVal < b.floatVal
    of dtString: a.stringVal < b.stringVal
    of dtBool: a.boolVal < b.boolVal
    of dtDateTime: a.dateTimeVal < b.dateTimeVal

proc `+`*(a, b: Value): Value =
    case a.kind
    of dtInt:
        if b.kind == dtInt: newValue(a.intVal + b.intVal)
        elif b.kind == dtFloat: newValue(a.intVal.float64 + b.floatVal)
        else: raise newException(ValueError, "Cannot add " & $a.kind & " and " & $b.kind)
    of dtFloat:
        if b.kind == dtInt: newValue(a.floatVal + b.intVal.float64)
        elif b.kind == dtFloat: newValue(a.floatVal + b.floatVal)
        else: raise newException(ValueError, "Cannot add " & $a.kind & " and " & $b.kind)
    else: raise newException(ValueError, "Addition not supported for " & $a.kind)

proc `-`*(a, b: Value): Value =
    case a.kind
    of dtInt:
        if b.kind == dtInt: newValue(a.intVal - b.intVal)
        elif b.kind == dtFloat: newValue(a.intVal.float64 - b.floatVal)
        else: raise newException(ValueError, "Cannot subtract " & $b.kind &
                " from " & $a.kind)
    of dtFloat:
        if b.kind == dtInt: newValue(a.floatVal - b.intVal.float64)
        elif b.kind == dtFloat: newValue(a.floatVal - b.floatVal)
        else: raise newException(ValueError, "Cannot subtract " & $b.kind &
                " from " & $a.kind)
    else: raise newException(ValueError, "Subtraction not supported for " & $a.kind)

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

proc newSeries*(data: seq[float64], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtFloat
    )

proc newSeries*(data: seq[string], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtString
    )

proc newSeries*(data: seq[bool], name = ""): Series = newSeriesWithDataType(
        data.mapIt(newValue(it)), name, dtBool
    )

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

proc info*(df: DataFrame): string =
    result = "DataFrame Info:\n"
    result.add("Shape: " & $df.shape & "\n")
    result.add("Columns: " & $df.columns.len & "\n")
    for name, series in df.columns:
        result.add("  " & name & ": " & $series.dtype & " (" & $series.len & " values)\n")

proc dropna*(df: DataFrame): DataFrame =
    result = newDataFrame()
    var validRows: seq[int] = @[]

    for i in 0..<df.len:
        var hasNull = false
        for _, series in df.columns:
            ## TODO: Handle NaN.
            if not hasNull:
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

proc groupBy*(df: DataFrame, by: string): OrderedTable[string, DataFrame] =
    if by notin df.columns:
        raise newException(KeyError, "Column not found: " & by)

    var groups = initOrderedTable[string, seq[int]]()
    let groupSeries = df.columns[by]

    for i, val in groupSeries.data:
        let key = $val
        if key notin groups:
            groups[key] = @[]
        groups[key].add(i)

    result = initOrderedTable[string, DataFrame]()
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
        result[key] = groupDf

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
    ## Optionally: The delimiter and whether or not to utilize index.
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
    ## Slice the DataFrame by row range using Nim's Slice syntax.
    let startRow = rowSlice.a
    let endRow = rowSlice.b + 1
    result = df.slice(startRow, endRow, 0, -1)

proc slice*(df: DataFrame, colNames: seq[string]): DataFrame =
    ## Slices the DataFrame by column names.
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
    fillValues: OrderedTable[string, Value]): DataFrame =
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

export
    DataType,
    Value,
    Series,
    DataFrame,
    newSeriesWithDataType,
    newSeries,
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
    dropna,
    groupBy,
    dateRange,
    resample,
    readCsv,
    toCsv,
    toJson,
    toParquet,
    fillNa,
    dropNa
