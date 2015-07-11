# orientpackets_pack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import macros
from rawsockets import htons, htonl

import orienttypes

proc pack*(buffer: var OrientPacket, data: OrientBoolean) =
    var networkData: OrientByte = cast[OrientByte](data)
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

proc pack*(buffer: var OrientPacket, data: var OrientBytes) =
    buffer.pack(cast[OrientInt](data.len))

    copyMem(buffer.buffer, addr(data[0]), data.len)

    buffer += data.len

proc pack*(buffer: var OrientPacket, data: OrientString not nil) =
    var networkData: cstring = data
    var length: OrientInt = OrientInt(data.len)

    buffer.pack(length)

    copyMem(buffer.buffer, networkData, length)
    buffer += length

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
