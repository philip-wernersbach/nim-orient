# orienttypes.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import tables

type
    OrientBoolean*  = bool
    OrientByte*     = uint8
    OrientChar      = cchar
    OrientShort*    = int16
    OrientInt*      = int32
    OrientLong*     = int64
    OrientBytes*    = seq[OrientByte]
    OrientString*   = string
    OrientStrings*  = seq[OrientString]

    OrientVarInt*       = int64
    OrientLink*         = OrientBytes
    OrientLinks*        = seq[OrientLink]

    # Unpacking an RIDBag is its own can of worms, so we allow people to
    # unpack them on-demand.
    OrientPackedTreeRIDBag* = tuple
        treePointer: OrientBytes
        changes:     OrientBytes

    OrientType* {.pure.} = enum
        Boolean      = 0
        Int          = 1
        Short        = 2
        Long         = 3
        #Float        = 4
        #Double       = 5
        #DateTime     = 6
        String       = 7
        Binary       = 8
        #Emb          = 9
        #EmbList      = 10
        #EmbdSet      = 11
        #EmbeMap      = 12
        #Link         = 13
        #LinkList     = 14
        #LinkSet      = 15
        #LinkMap      = 16
        Byte         = 17
        #Trans        = 18
        #Date         = 19
        #Custom       = 20
        #Decimal      = 21
        LinkBag      = 22
        #Any          = 23

        PackedTreeRIDBag = 255

    OrientRecord*   = tuple
        recordType:                 OrientChar
        clusterID:                  OrientShort
        clusterPosition:            OrientLong
        recordVersion:              OrientInt
        recordSerializationVersion: OrientByte
        recordClassName:            OrientString
        recordContent:              OrientPacket
    OrientRecords*  = seq[OrientRecord]

    OrientVariantObj* = object of RootObj
        case typ*: OrientType
        of OrientType.Boolean:          dataBoolean*:          OrientBoolean
        of OrientType.Int:              dataInt*:              OrientInt
        of OrientType.Short:            dataShort*:            OrientShort
        of OrientType.Long:             dataLong*:             OrientLong
        of OrientType.String:           dataString*:           OrientString
        of OrientType.Binary:           dataBinary*:           OrientBytes
        of OrientType.Byte:             dataByte*:             OrientByte
        of OrientType.LinkBag:          dataLinks*:            OrientLinks
        of OrientType.PackedTreeRIDBag: dataPackedTreeRIDBag*: OrientPackedTreeRIDBag
        else:
            discard
    OrientVariant* = ref OrientVariantObj

    OrientUnpackedFields*  = Table[OrientString, OrientVariant]
    OrientUnpackedRecords* = tuple
        fields:    OrientUnpackedFields

    OrientClusters* = Table[OrientString, OrientShort]

    OrientPacket* = tuple
        data:   seq[uint8]
        cursor: int

    OrientOperationFailed* = object of Exception
    OrientDBOpenFailed* = object of OrientOperationFailed
    OrientCommandFailed* = object of OrientOperationFailed

    OrientDBFeatureUnsupportedInLibrary* = object of Exception
    OrientServerBug* = object of Exception

proc buffer*(packet: var OrientPacket): pointer {.noSideEffect.} =
    return addr(packet.data[packet.cursor])

proc `+=`*(buffer: var OrientPacket, y: int) {.noSideEffect.} =
    buffer.cursor += y

proc reset*(buffer: var OrientPacket) {.noSideEffect.} =
    buffer.cursor = 0

proc newOrientPacket*(length: int): OrientPacket {.noSideEffect.} =
    return (data: newSeq[uint8](length), cursor: 0)

proc newOrientPacket*(data: OrientBytes): OrientPacket {.noSideEffect.} =
    return (data: data, cursor: 0)

proc newOrientRecord*(recordType: OrientByte, clusterID: OrientShort, clusterPosition: OrientLong, recordVersion: OrientInt, recordSerializationVersion: OrientByte, recordClassName: OrientString, recordContent: OrientPacket): OrientRecord {.noSideEffect.} =
    return (recordType: OrientChar(recordType), clusterID: clusterID, clusterPosition: clusterPosition, recordVersion: recordVersion, recordSerializationVersion: recordSerializationVersion, recordClassName: recordClassName, recordContent: recordContent)

proc newOrientRecord*(recordType: OrientByte, clusterID: OrientShort, clusterPosition: OrientLong, recordVersion: OrientInt, recordSerializationVersion: OrientByte, recordClassName: OrientString, recordContent: OrientBytes): OrientRecord {.noSideEffect.} =
    return newOrientRecord(recordType, clusterID, clusterPosition, recordVersion, recordSerializationVersion, recordClassName, newOrientPacket(recordContent))
