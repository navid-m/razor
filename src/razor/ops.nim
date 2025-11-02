import
    models,
    times,
    values

proc `$`*(v: Value): string =
    case v.kind
    of dtInt: $v.intVal
    of dtFloat: $v.floatVal
    of dtString: v.stringVal
    of dtBool: $v.boolVal
    of dtDateTime: $v.dateTimeVal
    of dtNa: "na"

proc `==`*(a, b: Value): bool =
    if a.kind != b.kind: return false
    case a.kind
    of dtInt: a.intVal == b.intVal
    of dtFloat: abs(a.floatVal - b.floatVal) < 1e-10
    of dtString: a.stringVal == b.stringVal
    of dtBool: a.boolVal == b.boolVal
    of dtDateTime: a.dateTimeVal == b.dateTimeVal
    of dtNa: false

proc `<`*(a, b: Value): bool =
    if a.kind != b.kind: return false
    case a.kind
    of dtInt: a.intVal < b.intVal
    of dtFloat: a.floatVal < b.floatVal
    of dtString: a.stringVal < b.stringVal
    of dtBool: a.boolVal < b.boolVal
    of dtDateTime: a.dateTimeVal < b.dateTimeVal
    of dtNa: false

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

proc `+`*(a, b: Series): Series =
    ## Element-wise addition of two Series
    if a.data.len != b.data.len:
        raise newException(ValueError, "Series must have the same length for arithmetic operations")

    var resultData = newSeq[Value](a.data.len)
    var resultIndex: seq[string] = @[]
    
    if a.index.len == a.data.len:
        resultIndex = a.index
    elif b.index.len == a.data.len:
        resultIndex = b.index

    for i in 0..<a.data.len:
        resultData[i] = a.data[i] + b.data[i]

    var resultDtype = a.dtype
    if a.dtype == dtInt and b.dtype == dtFloat:
        resultDtype = dtFloat
    elif a.dtype == dtFloat and b.dtype == dtInt:
        resultDtype = dtFloat
    elif a.dtype == dtFloat and b.dtype == dtFloat:
        resultDtype = dtFloat

    Series(
        data: resultData,
        name: "",
        dtype: resultDtype,
        index: resultIndex
    )


proc `*`*(a, b: Value): Value =
    case a.kind
    of dtInt:
        if b.kind == dtInt: newValue(a.intVal * b.intVal)
        elif b.kind == dtFloat: newValue(a.intVal.float64 * b.floatVal)
        else: raise newException(ValueError, "Cannot multiply " & $a.kind &
                " and " & $b.kind)
    of dtFloat:
        if b.kind == dtInt: newValue(a.floatVal * b.intVal.float64)
        elif b.kind == dtFloat: newValue(a.floatVal * b.floatVal)
        else: raise newException(ValueError, "Cannot multiply " & $a.kind &
                " and " & $b.kind)
    else: raise newException(ValueError, "Multiplication not supported for " & $a.kind)


proc `-`*(a, b: Series): Series =
    ## Element-wise subtraction of two Series
    if a.data.len != b.data.len:
        raise newException(ValueError, "Series must have the same length for arithmetic operations")

    var resultData = newSeq[Value](a.data.len)
    var resultIndex: seq[string] = @[]

    if a.index.len == a.data.len:
        resultIndex = a.index
    elif b.index.len == a.data.len:
        resultIndex = b.index

    for i in 0..<a.data.len:
        resultData[i] = a.data[i] - b.data[i]

    var resultDtype = a.dtype
    if a.dtype == dtInt and b.dtype == dtFloat:
        resultDtype = dtFloat
    elif a.dtype == dtFloat and b.dtype == dtInt:
        resultDtype = dtFloat
    elif a.dtype == dtFloat and b.dtype == dtFloat:
        resultDtype = dtFloat

    Series(
        data: resultData,
        name: "",
        dtype: resultDtype,
        index: resultIndex
    )

proc `*`*(a, b: Series): Series =
    ## Element-wise multiplication of two Series
    if a.data.len != b.data.len:
        raise newException(ValueError, "Series must have the same length for arithmetic operations")

    var resultData = newSeq[Value](a.data.len)
    var resultIndex: seq[string] = @[]
    
    if a.index.len == a.data.len:
        resultIndex = a.index
    elif b.index.len == a.data.len:
        resultIndex = b.index

    for i in 0..<a.data.len:
        resultData[i] = a.data[i] * b.data[i]

    var resultDtype = a.dtype
    if a.dtype == dtInt and b.dtype == dtFloat:
        resultDtype = dtFloat
    elif a.dtype == dtFloat and b.dtype == dtInt:
        resultDtype = dtFloat
    elif a.dtype == dtFloat and b.dtype == dtFloat:
        resultDtype = dtFloat

    Series(
        data: resultData,
        name: "",
        dtype: resultDtype,
        index: resultIndex
    )


proc `/`*(a, b: Value): Value =
    case a.kind
    of dtInt:
        if b.kind == dtInt:
            if b.intVal == 0: raise newException(DivByZeroDefect, "Division by zero")
            newValue(a.intVal.float64 / b.intVal.float64)
        elif b.kind == dtFloat:
            if b.floatVal == 0.0: raise newException(DivByZeroDefect, "Division by zero")
            newValue(a.intVal.float64 / b.floatVal)
        else: raise newException(ValueError, "Cannot divide " & $a.kind &
                " by " & $b.kind)
    of dtFloat:
        if b.kind == dtInt:
            if b.intVal == 0: raise newException(DivByZeroDefect, "Division by zero")
            newValue(a.floatVal / b.intVal.float64)
        elif b.kind == dtFloat:
            if b.floatVal == 0.0: raise newException(DivByZeroDefect, "Division by zero")
            newValue(a.floatVal / b.floatVal)
        else: raise newException(ValueError, "Cannot divide " & $a.kind &
                " by " & $b.kind)
    else: raise newException(ValueError, "Division not supported for " & $a.kind)


proc `/`*(a, b: Series): Series =
    ## Element-wise division of two Series (always returns float)
    if a.data.len != b.data.len:
        raise newException(ValueError, "Series must have the same length for arithmetic operations")

    var resultData = newSeq[Value](a.data.len)
    var resultIndex: seq[string] = @[]
    
    if a.index.len == a.data.len:
        resultIndex = a.index
    elif b.index.len == a.data.len:
        resultIndex = b.index

    for i in 0..<a.data.len:
        resultData[i] = a.data[i] / b.data[i]

    Series(
        data: resultData,
        name: "",
        dtype: dtFloat,
        index: resultIndex
    )

proc `==`*(values: seq[Value], expected: seq[int64]): bool =
    if values.len != expected.len: return false
    for i in 0..<values.len:
        if values[i] != newValue(expected[i]): return false
    true
