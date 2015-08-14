# orientpackets_pack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unsigned
import macros
from rawsockets import htons, htonl

import orienttypes
import orientsystemextensions

proc pack*(buffer: var OrientPacket, data: OrientBoolean) =
    var networkData: OrientByte

    if data != false:
        networkData = 0
    else:
        networkData = 1

    copyMem(buffer.buffer, addr(networkData), sizeof(OrientByte))

    buffer += sizeof(OrientByte)

proc pack*(buffer: var OrientPacket, data: OrientByte) =
    var networkData = data
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientByte))

    buffer += sizeof(OrientByte)

proc pack*(buffer: var OrientPacket, data: OrientShort) =
    var networkData = data.htons
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientShort))

    buffer += sizeof(OrientShort)

proc pack*(buffer: var OrientPacket, data: OrientInt) =
    var networkData = data.htonl
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientInt))

    buffer += sizeof(OrientInt)

proc packLong*(buffer: var OrientPacket, data: OrientLong) =
    var networkData = data.htonll
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientLong))

    buffer += sizeof(OrientLong)

proc pack*(buffer: var OrientPacket, data: var OrientBytes) =
    buffer.pack(cast[OrientInt](data.len))

    copyMem(buffer.buffer, addr(data[0]), data.len)
    buffer += data.len

template packLink*(buffer: var OrientPacket, data: var OrientLink) =
    buffer.pack(data)

proc pack*(buffer: var OrientPacket, data: OrientString not nil) =
    var networkData: cstring = data
    let length: OrientInt = OrientInt(data.len)

    buffer.pack(length)

    copyMem(buffer.buffer, networkData, length)
    buffer += length

proc pack*(buffer: var OrientPacket, data: var OrientPacket) =
    buffer.pack(cast[OrientInt](data.cursor))

    copyMem(buffer.buffer, addr(data.data[0]), data.cursor)
    buffer += data.cursor

# We always pack varints in 10-bits. It's simpler and faster.
proc packZigZagNum(data: uint64): uint64 =
    result = (data shl 1) xor (data shr 63)

proc pack*(buffer: var OrientPacket, data: OrientVarInt) =
    let dataU = packZigZagNum(cast[uint64](data))

    let part0: uint8 = cast[uint8](dataU) or 0x80'u8
    let part1: uint8 = cast[uint8](dataU shr 7) or 0x80'u8
    let part2: uint8 = cast[uint8](dataU shr 14) or 0x80'u8
    let part3: uint8 = cast[uint8](dataU shr 21) or 0x80'u8
    let part4: uint8 = cast[uint8](dataU shr 28) or 0x80'u8
    let part5: uint8 = cast[uint8](dataU shr 35) or 0x80'u8
    let part6: uint8 = cast[uint8](dataU shr 42) or 0x80'u8
    let part7: uint8 = cast[uint8](dataU shr 49) or 0x80'u8
    let part8: uint8 = cast[uint8](dataU shr 56) or 0x80'u8
    let part9: uint8 = cast[uint8](dataU shr 63)

    buffer.pack(part0)
    buffer.pack(part1)
    buffer.pack(part2)
    buffer.pack(part3)
    buffer.pack(part4)
    buffer.pack(part5)
    buffer.pack(part6)
    buffer.pack(part7)
    buffer.pack(part8)
    buffer.pack(part9)


macro packAll*(buffer: var OrientPacket, dataVars: varargs[untyped]): untyped =
    var blockContents = newStmtList(newVarStmt(newIdentNode("length"), newIntLitNode(0)))

    for data in dataVars.children:
        case data.typeKind():
        of ntySequence, ntyString:
            blockContents.add(newCall("+=", newIdentNode("length"), newIntLitNode(sizeof(OrientInt))))
            blockContents.add(newCall("+=", newIdentNode("length"), newCall(newIdentNode("len"), data)))
        else:
            blockContents.add(newCall("+=", newIdentNode("length"), newCall(newIdentNode("sizeof"), data.getType())))

    blockContents.add(newAssignment(buffer, newCall(newIdentNode("newOrientPacket"), newIdentNode("length"))))

    for data in dataVars.children:
        blockContents.add(newCall(newIdentNode("pack"), buffer, data))

    return newBlockStmt(blockContents)


# packBytesVIL stands for packBytesVarIntLength
proc packBytesVIL*(buffer: var OrientPacket, data: var OrientBytes) =
    let length = data.len

    buffer.pack(OrientVarInt(length))

    copyMem(buffer.buffer, addr(data[0]), length)
    buffer += length

# packStringVIL stands for packStringVarIntLength
proc packStringVIL*(buffer: var OrientPacket, data: OrientString not nil) =
    var networkData: cstring = data
    let length = data.len

    buffer.pack(OrientVarInt(length))

    copyMem(buffer.buffer, networkData, length)
    buffer += length
