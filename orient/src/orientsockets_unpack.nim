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
from rawsockets import ntohs, ntohl

import orienttypes
import orientsystemextensions
import orientpackets_unpack

proc unpackByte*(socket: Socket): OrientByte =
    discard socket.recv(addr(result), sizeof(OrientByte))

proc unpackBoolean*(socket: Socket): OrientBoolean =
    var buffer: OrientBoolean

    discard socket.recv(addr(buffer), sizeof(OrientBoolean))

    if int(buffer) != 0:
        result = false
    else:
        result = true

proc unpackShort*(socket: Socket): OrientShort =
    discard socket.recv(addr(result), sizeof(OrientShort))
    result = result.ntohs

proc unpackInt*(socket: Socket): OrientInt =
    discard socket.recv(addr(result), sizeof(OrientInt))
    result = result.ntohl

proc unpackLong*(socket: Socket): OrientLong =
    discard socket.recv(addr(result), sizeof(OrientLong))
    result = result.ntohll

proc unpackBytes*(socket: Socket, length: int): OrientBytes not nil =
    let wireLength = length * sizeof(OrientByte)
    result = cast[OrientBytes not nil](OrientBytes(newSeq[OrientByte](wireLength)))

    if length > 0:
        discard socket.recv(addr(result[0]), wireLength)

proc unpackBytes*(socket: Socket): OrientBytes not nil =
    result = socket.unpackBytes(socket.unpackInt)

proc unpackString*(socket: Socket): OrientString =
    let stringWireLength = sizeof(cchar) * socket.unpackInt

    result = newString(stringWireLength)
    discard socket.recv(addr(result[0]), stringWireLength)

proc unpackClusters*(socket: Socket): OrientClusters =
    let clustersLeft = socket.unpackShort
    result = initTable[OrientString, OrientShort](clustersLeft.nextPowerOfTwo)

    for i in countDown(clustersLeft, 1):
        let clusterName = socket.unpackString
        let clusterId = socket.unpackShort
        result[clusterName] = clusterId

    # We don't support cluster-config
    let clusterConfigLength = socket.unpackInt

    if clusterConfigLength > 0:
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support server clustered configurations!")

proc unpackRecord*(socket: Socket): OrientRecord =
    let recordFormat = socket.unpackShort
    if recordFormat != 0:
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support records of format \"" & recordFormat.intToStr() & "\"!")

    let recordType = socket.unpackByte
    let clusterID = socket.unpackShort
    let clusterPosition = socket.unpackLong
    let recordVersion = socket.unpackInt

    if recordType != OrientByte('d'):
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support record-types other than \"d\"!")

    var recordContent = newOrientPacket(socket.unpackBytes)

    let recordSerializationVersion = recordContent.unpackByte
    let recordClassName = recordContent.unpackString(int(recordContent.unpackVarInt))

    result = newOrientRecord(recordType, clusterID, clusterPosition, recordVersion, recordSerializationVersion, recordClassName, recordContent)
