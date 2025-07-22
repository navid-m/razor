import
    tables,
    times

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

    GroupedDataFrame* = ref object
        groups*: OrderedTable[string, DataFrame]
        groupByColumn*: string
