# orientsockets_unpack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import tables
import math
import net
import strutils

import orienttypes
import orientpackets_unpack

proc unpackByte*(socket: Socket): OrientByte =
    var buffer = newOrientPacket(sizeof(OrientByte))
    discard socket.recv(buffer.buffer, sizeof(OrientByte))

    return buffer.unpackByte

proc unpackBoolean*(socket: Socket): OrientBoolean =
    var buffer = newOrientPacket(sizeof(OrientBoolean))
    discard socket.recv(buffer.buffer, sizeof(OrientBoolean))

    return buffer.unpackBoolean

proc unpackShort*(socket: Socket): OrientShort =
    var buffer = newOrientPacket(sizeof(OrientShort))
    discard socket.recv(buffer.buffer, sizeof(OrientShort))

    return buffer.unpackShort

proc unpackInt*(socket: Socket): OrientInt =
    var buffer = newOrientPacket(sizeof(OrientInt))
    discard socket.recv(buffer.buffer, sizeof(OrientInt))

    return buffer.unpackInt

proc unpackLong*(socket: Socket): OrientLong =
    var buffer = newOrientPacket(sizeof(OrientLong))
    discard socket.recv(buffer.buffer, sizeof(OrientLong))

    return buffer.unpackLong

proc unpackBytes*(socket: Socket, length: int): OrientBytes not nil =
    let wireLength = length * sizeof(OrientByte)
    var buffer = newOrientPacket(wireLength)
    var unpacked = cast[OrientBytes not nil](newSeq[OrientByte](0))

    if length > 0:
        discard socket.recv(buffer.buffer, wireLength)
        unpacked = buffer.unpackBytes(wireLength)

    return unpacked

proc unpackBytes*(socket: Socket): OrientBytes not nil =
    return socket.unpackBytes(socket.unpackInt)

proc unpackString*(socket: Socket): OrientString =
    let stringWireLength = sizeof(cchar) * socket.unpackInt

    var buffer = newOrientPacket(stringWireLength)
    discard socket.recv(buffer.buffer, stringWireLength)

    return buffer.unpackString(stringWireLength)

proc unpackClusters*(socket: Socket): OrientClusters =
    let clustersLeft = socket.unpackShort
    var clusters = initTable[OrientString, OrientShort](clustersLeft.nextPowerOfTwo)

    for i in countDown(clustersLeft, 1):
        let clusterName = socket.unpackString
        let clusterId = socket.unpackShort
        clusters[clusterName] = clusterId

    # We don't support cluster-config
    let clusterConfigLength = socket.unpackInt

    if clusterConfigLength > 0:
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support server clustered configurations!")

    return clusters

proc unpackRecord*(socket: Socket): OrientRecord =
    let recordFormat = socket.unpackShort
    if recordFormat != 0:
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support records of format \"" & recordFormat.intToStr() & "\"!")

    let recordType = socket.unpackByte
    let clusterID = socket.unpackShort
    let clusterPosition = socket.unpackLong
    let recordVersion = socket.unpackInt
    let recordContent = socket.unpackBytes

    return newOrientRecord(recordType, clusterID, clusterPosition, recordVersion, recordContent)
