# orientrecords_unpack.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unsigned
import strutils
import tables

import orienttypes
import orientpackets_unpack

proc unpack*(record: var OrientRecord): OrientUnpackedRecords =
    result = (fields: initTable[OrientString, OrientVariant]())

    while true:
        let fieldNameLength = int(record.recordContent.unpackVarInt)
        if fieldNameLength == 0:
            break
        elif fieldNameLength < 0:
           raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support negative field_name_lengths!")

        let fieldName = record.recordContent.unpackString(fieldNameLength)
        let pointerToDataStructure = record.recordContent.unpackInt
        let dataType = record.recordContent.unpackByte

        var unpackedField: OrientVariant
        new(unpackedField)

        let oldCursor = record.recordContent.cursor
        record.recordContent.cursor = pointerToDataStructure

        try:
            case OrientType(dataType)
            of OrientType.Boolean:
                unpackedField.typ = OrientType.Boolean
                unpackedField.dataBoolean = record.recordContent.unpackBoolean
            of OrientType.Int:
                unpackedField.typ = OrientType.Int
                unpackedField.dataInt = OrientInt(record.recordContent.unpackVarInt)
            of OrientType.Short:
                unpackedField.typ = OrientType.Short
                unpackedField.dataShort = OrientShort(record.recordContent.unpackVarInt)
            of OrientType.Long:
                unpackedField.typ = OrientType.Long
                unpackedField.dataLong = OrientLong(record.recordContent.unpackVarInt)
            of OrientType.String:
                unpackedField.typ = OrientType.String

                let size = record.recordContent.unpackVarInt
                unpackedField.dataString = record.recordContent.unpackString(int(size))
            of OrientType.Binary:
                unpackedField.typ = OrientType.Binary

                let size = record.recordContent.unpackVarInt
                unpackedField.dataBinary = record.recordContent.unpackBytes(int(size))
            of OrientType.Byte:
                unpackedField.typ = OrientType.Byte
                unpackedField.dataByte = record.recordContent.unpackByte
            of OrientType.LinkBag:
                let config = record.recordContent.unpackByte
                let bagTyp: uint8 = config and 1
                let isUUIDPresent: uint8 = (config shl 1) and 1

                if isUUIDPresent != 0:
                    raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support LinkBags with UUIDs!")

                if bagTyp == 1:
                    unpackedField.typ = OrientType.LinkBag

                    let size = record.recordContent.unpackInt
                    var bag = newSeq[OrientLink](size)

                    for i in countUp(0, size-1):
                        bag[i] = record.recordContent.unpackBytes(10)

                    unpackedField.dataLinks = bag
                else:
                    let treePointerLength = sizeof(OrientLong) + sizeof(OrientLong) + sizeof(OrientInt)
                    let singleChangeLength = 10 + sizeof(OrientByte) + sizeof(OrientInt)

                    unpackedField.typ = OrientType.PackedTreeRIDBag

                    let treePointer = record.recordContent.unpackBytes(treePointerLength)

                    # No idea why they even bother to include the size field for tree-based RID Bags, its unused.
                    discard record.recordContent.unpackInt

                    # Save the current cursor so that we can rewind and get the full changes bytes, including the length.
                    let fullChangesCursor = record.recordContent.cursor

                    let changesSize = record.recordContent.unpackInt

                    record.recordContent.cursor = fullChangesCursor
                    let changes = record.recordContent.unpackBytes(sizeof(OrientInt) + (changesSize * singleChangeLength))

                    unpackedField.dataPackedTreeRIDBag.treePointer = treePointer
                    unpackedField.dataPackedTreeRIDBag.changes = changes
            of OrientType.PackedTreeRIDBag:
                # Not possible, internal type but not a network type
                raise newException(OrientServerBug, "Internal type number \"" & int(dataType).intToStr(1) & "\" used as network type!")

        except RangeError:
            raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support data_types of \"" & int(dataType).intToStr(1) & "\"!")
        finally:
            record.recordContent.cursor = oldCursor

        result.fields[fieldName] = unpackedField

iterator unpackEach*(records: var OrientRecords): OrientUnpackedRecords =
    for record in records:
        var recordVar = record
        yield recordVar.unpack
