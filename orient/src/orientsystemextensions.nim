# orientsystemextensions.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

proc ntohll*(x: int64): int64 =
  ## Converts 64-bit integers from network to host byte order.
  ## On machines where the host byte order is the same as network byte order,
  ## this is a no-op; otherwise, it performs an 8-byte swap operation.
  when cpuEndian == bigEndian: result = x
  else: result = (x shr 56)                      or
                 (x shr 40 and 0xff00)           or
                 (x shr 24 and 0xff0000)         or
                 (x shr  8 and 0xff000000)       or
                 (x shl  8 and 0xff00000000)     or
                 (x shl 24 and 0xff0000000000)   or
                 (x shl 40 and 0xff000000000000) or
                 (x shl 56)

proc htonll*(x: int64): int64 =
  result = x.ntohll
