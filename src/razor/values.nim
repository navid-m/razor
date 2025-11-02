import
    models,
    times

proc newValue*(val: int64): Value = Value(kind: dtInt, intVal: val)
proc newValue*(val: float64): Value = Value(kind: dtFloat, floatVal: val)
proc newValue*(val: string): Value = Value(kind: dtString, stringVal: val)
proc newValue*(val: bool): Value = Value(kind: dtBool, boolVal: val)
proc newValue*(val: DateTime): Value = Value(kind: dtDateTime, dateTimeVal: val)
proc newNaValue*(): Value = Value(kind: dtNa)

template na*(): Value = newNaValue()

proc isNa*(v: Value): bool = v.kind == dtNa

template v*(val: int64): Value = newValue(val)
template v*(val: float64): Value = newValue(val)
template v*(val: string): Value = newValue(val)
template v*(val: bool): Value = newValue(val)
template v*(val: DateTime): Value = newValue(val)
