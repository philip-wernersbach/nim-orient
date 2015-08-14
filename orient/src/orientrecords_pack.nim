# orientrecords_pack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import tables

import orienttypes
import orientpackets_pack

# THIS IS UNSAFE! NEED TO IMPLEMENT SAFE VERSIONS OF pack*!
proc pack*(packedRecord: var OrientRecord, contents: OrientUnpackedRecords, estimatedRecordContentLength = 4096) =
    var pointersToDataStructures = initTable[OrientString, int](contents.fields.len.rightSize)

    let computedRecordContentLength = packedRecord.recordContent.data.len * 2
    if computedRecordContentLength < estimatedRecordContentLength:
        packedRecord.recordContent = newOrientPacket(estimatedRecordContentLength)
    else:
        packedRecord.recordContent = newOrientPacket(computedRecordContentLength)

    packedRecord.recordContent.pack(packedRecord.recordSerializationVersion)
    packedRecord.recordContent.packStringVIL(packedRecord.recordClassName)

    # Pack the header.
    for key, value in contents.fields.pairs:
        if key != nil:
            # Pack field_name_length and field_name
            packedRecord.recordContent.packStringVIL(key)

            # Pack pointer_to_data_structure with a zero
            pointersToDataStructures[key] = packedRecord.recordContent.cursor
            packedRecord.recordContent.pack(OrientInt(0))

            case value.typ:
            of OrientType.Boolean:
                packedRecord.recordContent.pack(OrientByte(OrientType.Boolean))
            of OrientType.Int:
                packedRecord.recordContent.pack(OrientByte(OrientType.Int))
            of OrientType.Short:
                packedRecord.recordContent.pack(OrientByte(OrientType.Short))
            of OrientType.Long:
                packedRecord.recordContent.pack(OrientByte(OrientType.Long))
            of OrientType.String:
                packedRecord.recordContent.pack(OrientByte(OrientType.String))
            of OrientType.Binary:
                packedRecord.recordContent.pack(OrientByte(OrientType.Binary))
            of OrientType.Link:
                packedRecord.recordContent.pack(OrientByte(OrientType.Link))
            of OrientType.Byte:
                packedRecord.recordContent.pack(OrientByte(OrientType.Byte))
            of OrientType.LinkBag, OrientType.PackedTreeRIDBag:
                packedRecord.recordContent.pack(OrientByte(OrientType.LinkBag))
        else:
            raise newException(AssertionError, "OrientRecord field name cannot be nil!")

    # Pack End-Of-Header Delimiter
    packedRecord.recordContent.pack(OrientVarInt(0))

    # Pack data
    for key, value in contents.fields.pairs:
        if key != nil:
            # Save current cursor
            let oldCursor = packedRecord.recordContent.cursor

            # Pack pointer_to_data_structure with actual value
            packedRecord.recordContent.cursor = pointersToDataStructures[key]
            packedRecord.recordContent.pack(OrientInt(oldCursor))

            # Restore cursor
            packedRecord.recordContent.cursor = oldCursor

            # Pack data value.
            case value.typ:
            of OrientType.Boolean:
                packedRecord.recordContent.pack(value.dataBoolean)
            of OrientType.Int:
                packedRecord.recordContent.pack(OrientVarInt(value.dataInt))
            of OrientType.Short:
                packedRecord.recordContent.pack(OrientVarInt(value.dataShort))
            of OrientType.Long:
                packedRecord.recordContent.pack(OrientVarInt(value.dataLong))
            of OrientType.String:
                packedRecord.recordContent.packStringVIL(value.dataString)
            of OrientType.Binary:
                packedRecord.recordContent.packBytesVIL(value.dataBinary)
            of OrientType.Link:
                packedRecord.recordContent.packLink(value.dataLink)
            of OrientType.Byte:
                packedRecord.recordContent.pack(value.dataByte)
            of OrientType.LinkBag:
                # Pack the config with the setting for embedded bag with no UUID.
                packedRecord.recordContent.pack(OrientByte(1))

                # Pack the size.
                packedRecord.recordContent.pack(OrientInt(value.dataLinks.len))

                for link in value.dataLinks.mitems:
                    packedRecord.recordContent.pack(OrientBytes(link))
            of OrientType.PackedTreeRIDBag:
                # Pack the config with the setting for tree-based with no UUID.
                packedRecord.recordContent.pack(OrientByte(0))

                # Pack the tree pointer.
                packedRecord.recordContent.pack(value.dataPackedTreeRIDBag.treePointer)

                # Pack the size.
                packedRecord.recordContent.pack(OrientInt(value.dataPackedTreeRIDBag.changes.len))

                # Pack the number of changes and the changes.
                packedRecord.recordContent.pack(value.dataPackedTreeRIDBag.changes)
        else:
            raise newException(AssertionError, "OrientRecord field name cannot be nil!")
