# orientpackets_pack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from rawsockets import htons, htonl

import orienttypes

proc pack*(buffer: var OrientPacket, data: OrientBoolean): var OrientPacket =
    var networkData: OrientByte = cast[OrientByte](data)
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientByte))

    buffer += sizeof(OrientByte)
    return buffer

proc pack*(buffer: var OrientPacket, data: OrientByte): var OrientPacket =
    var networkData = data
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientByte))

    buffer += sizeof(OrientByte)
    return buffer

proc pack*(buffer: var OrientPacket, data: OrientShort): var OrientPacket =
    var networkData = data.htons
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientShort))

    buffer += sizeof(OrientShort)
    return buffer

proc pack*(buffer: var OrientPacket, data: OrientInt): var OrientPacket =
    var networkData = data.htonl
    copyMem(buffer.buffer, addr(networkData), sizeof(OrientInt))

    buffer += sizeof(OrientInt)
    return buffer

proc pack*(buffer: var OrientPacket, data: var OrientBytes): var OrientPacket =
    discard buffer.pack(cast[OrientInt](data.len))

    copyMem(buffer.buffer, addr(data[0]), data.len)

    buffer += data.len
    return buffer

proc pack*(buffer: var OrientPacket, data: OrientString): var OrientPacket =
    var networkData: cstring = data
    var length: OrientInt = 0

    if data != nil:
        length = cast[OrientInt](data.len)

    discard buffer.pack(length)

    if length > 0:
        copyMem(buffer.buffer, networkData, length)
        buffer += length

    return buffer
