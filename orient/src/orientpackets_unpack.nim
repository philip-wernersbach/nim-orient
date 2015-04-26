# orientpackets_unpack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unsigned
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

# This could probably be done more efficiently with bit shifts, but unpackVarInt doesn't work for >1 byte anyways.
# TODO: Get someone that's smarter than me to implement this as bit shifts.
proc unpackZigZagNum[T](data: T): T =
    if (data mod 2) != 0:
        return cast[T](0) - ((data div 2) + 1)
    else:
        return data div 2

# I can't get this to work for values larger than one byte, oh well.
# TODO: Get someone that's smarter than me to make this work with >1 byte.
proc unpackVarInt*(buffer: var OrientPacket): OrientVarInt =
    var value: uint64 = 0
    var current: uint8
    var shift_multiplier: uint64 = 0

    while shift_multiplier < 7:
        current = (cast[ptr uint8](buffer.buffer))[]
        buffer += 1

        value = value or (cast[uint64](current and cast[uint8](0x7F)) shl (shift_multiplier * 7))

        if (current and cast[uint8](0x80)) == 0:
            break

        shift_multiplier += 1

    case cast[int](shift_multiplier):
    of 0:
        return cast[OrientVarInt](unpackZigZagNum(cast[uint8](value)))
    else:
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library only supports unpacking 8-bit varints!")

    return 0
