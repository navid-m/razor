import
    tables,
    times

type
    DataType* = enum
        dtInt,
        dtFloat,
        dtString,
        dtBool,
        dtDateTime,
        dtNa

    Value* = object
        case kind*: DataType
        of dtInt: intVal*: int64
        of dtFloat: floatVal*: float64
        of dtString: stringVal*: string
        of dtBool: boolVal*: bool
        of dtDateTime: dateTimeVal*: DateTime
        of dtNa: discard

    Series* = ref object
        data*: seq[Value]
        name*: string
        dtype*: DataType
        index*: seq[string]

    DataFrame* = ref object
        columns*: OrderedTable[string, Series]
        index*: seq[string]
        shape*: tuple[rows, cols: int]

    GroupedDataFrame* = object
        groups*: OrderedTable[string, DataFrame]
        groupByColumn*: string
