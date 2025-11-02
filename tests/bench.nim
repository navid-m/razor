import
    times,
    strformat,
    sequtils,
    razor

let start = cpuTime()
var df = newDataFrame()
df["id"] = newSeries((0..<10_000_000).toSeq)
df["value"] = newSeries(toSeq(0..<10_000_000).map(proc(x: int): float = x.float * 1.5))
echo &"Created in {cpuTime() - start:.3f} seconds"
