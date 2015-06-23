# orientpackets_unpack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unsigned
import macros
from rawsockets import ntohs, ntohl

import orienttypes

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

proc unpackByte*(buffer: var OrientPacket): OrientByte =
    var unpacked: OrientByte
    copyMem(addr(unpacked), buffer.buffer, sizeof(OrientByte))

    buffer += sizeof(OrientByte)

    return unpacked

proc unpackBoolean*(buffer: var OrientPacket): OrientBoolean =
    var unpacked: OrientByte = unpackByte(buffer)

    if cast[int](unpacked) != 0:
        return false
    else:
        return true

proc unpackShort*(buffer: var OrientPacket): OrientShort =
    var unpacked: OrientShort
    copyMem(addr(unpacked), buffer.buffer, sizeof(OrientShort))

    buffer += sizeof(OrientShort)

    return unpacked.ntohs

proc unpackInt*(buffer: var OrientPacket): OrientInt =
    var unpacked: OrientInt
    copyMem(addr(unpacked), buffer.buffer, sizeof(OrientInt))

    buffer += sizeof(OrientInt)

    return unpacked.ntohl

proc unpackLong*(buffer: var OrientPacket): OrientLong =
    var unpacked: OrientLong
    copyMem(addr(unpacked), buffer.buffer, sizeof(OrientLong))

    buffer += sizeof(OrientLong)

    return unpacked.ntohll

proc unpackBytes*(buffer: var OrientPacket, length: int): OrientBytes not nil =
    var unpacked: OrientBytes not nil

    if length > 0:
        unpacked = cast[OrientBytes not nil](newSeq[OrientByte](length))
        copyMem(addr(unpacked[0]), buffer.buffer, length)
        buffer += length
    else:
        unpacked = cast[OrientBytes not nil](newSeq[OrientByte](0))

    return unpacked

proc unpackString*(buffer: var OrientPacket, length: int): OrientString =
    var unpacked = newString(length)

    copyMem(addr(unpacked[0]), buffer.buffer, length)
    buffer += length

    return unpacked

# OrientDB's binary protocol uses varints + ZigZag encoding.
# This saves a trivial amount of space but increases computational complexity, why oh why do they do this to us?

# unpackVarIntLogic is used by unpackVarInt so that we don't have to repeat ourselves for each distinct
# varint length (0-10). The macro generates code like this for a two byte varint:
#
# let part0: uint64 = (cast[ptr uint8](buffer.buffer))[] and 0x7F'u8
# buffer += 1
#
# let part1: uint64 = (cast[ptr uint8](buffer.buffer))[]
# buffer += 1
#
# return return OrientVarInt(unpackZigZagNum(part0 or (part1 shl 7)))
#
# We call this once for each distinct varint length.
macro unpackVarIntLogic(ii: int): stmt =
    var logic = ""

    let i = intVal(ii) - 1

    for j in countUp(0, i - 1):
        logic &= "let part" & $j & ": uint64 = (cast[ptr uint8](buffer.buffer))[] and 0x7F'u8\n"
        logic &= "buffer += 1\n\n"

    logic &= "let part" & $i & ": uint64 = (cast[ptr uint8](buffer.buffer))[]\n"
    logic &= "buffer += 1\n\n"

    logic &= "return OrientVarInt(unpackZigZagNum(part0"

    for j in countUp(1, i):
        let shift = 7 * j
        logic &= " or (part" & $j & " shl " & $shift & ")"

    logic &= "))\n"

    ## Uncomment this to see the code that this macro generates.
    #debugEcho(logic)

    return parseStmt(logic)

proc unpackZigZagNum(data: uint64): uint64 =
    return (data shr 1) xor uint64((int64(data and 1) shl 63) shr 63)

proc unpackVarInt*(buffer: var OrientPacket): OrientVarInt =
    var current: uint8
    var length = 0

    let oldCursor = buffer.cursor

    while length <= 10:
        current = (cast[ptr uint8](buffer.buffer))[]
        buffer += 1
        length += 1

        if (current and 0x80'u8) == 0:
            break

    buffer.cursor = oldCursor

    case length
    of 0:
        return 0
    of 1:
        unpackVarIntLogic(1)
    of 2:
        unpackVarIntLogic(2)
    of 3:
        unpackVarIntLogic(3)
    of 4:
        unpackVarIntLogic(4)
    of 5:
        unpackVarIntLogic(5)
    of 6:
        unpackVarIntLogic(6)
    of 7:
        unpackVarIntLogic(7)
    of 8:
        unpackVarIntLogic(8)
    of 9:
        unpackVarIntLogic(9)
    of 10:
        unpackVarIntLogic(10)
    else:
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library only supports unpacking varints up to 64-bits in length!")
