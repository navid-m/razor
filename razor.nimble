# Package

version       = "0.1.0"
author        = "Navid M"
description   = "Pandas equivalent"
license       = "GPL-3.0-only"
srcDir        = "src"


# Dependencies

requires "nim >= 2.2.4"

task test, "Run all tests":
  exec "nim c -r tests/bench.nim"
  exec "nim c -r tests/df_tests.nim"
  exec "nim c -r tests/pd_tests.nim"
  exec "nim c -r tests/reg_test.nim"
