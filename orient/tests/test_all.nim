# test_all.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import net
import tables
import strutils

import orient

stdout.writeln("Connecting to database GratefulDeadConcerts on host localhost:2424 with username admin and password admin.")
let database = newOrientDatabase("GratefulDeadConcerts", "graph", "admin", "admin")
var connection = database.newOrientConnection("localhost", false, Port(2424))

try:
    stdout.writeln("Getting all verticies in database.")
    stdout.writeln("Contents of all returned records:")

    for item in connection.sqlQuery("select from V"):
        var packedRecord = item
        let record = packedRecord.unpack()

        stdout.writeln("-----------------")
        stdout.write(item.recordClassName)
        stdout.writeln(":")

        for name in record.fields.keys:
            stdout.write(name)
            stdout.write(" --> ")

            case record.fields[name].typ:
            of OrientType.Boolean:          stdout.writeln(record.fields[name].dataBoolean)
            of OrientType.Int:              stdout.writeln(record.fields[name].dataInt)
            of OrientType.Short:            stdout.writeln(record.fields[name].dataShort)
            of OrientType.Long:             stdout.writeln(record.fields[name].dataLong)
            of OrientType.String:           stdout.writeln(record.fields[name].dataString)
            of OrientType.Binary:           stdout.writeln($(record.fields[name].dataBinary))
            of OrientType.Link:             stdout.writeln($(record.fields[name].dataLink))
            of OrientType.Byte:             stdout.writeln(record.fields[name].dataByte)
            of OrientType.LinkBag:          stdout.writeln($(record.fields[name].dataLinks))
            of OrientType.PackedTreeRIDBag: stdout.writeln($(record.fields[name].dataPackedTreeRIDBag))

        stdout.writeln("-----------------")

    stdout.writeln("")

    stdout.writeln("We were successful, closing database connection and exiting.")
except:
    let e = getCurrentException()
    stderr.write(e.getStackTrace())
    stderr.write("Error: unhandled exception: ")
    stderr.writeln(getCurrentExceptionMsg())

    stderr.writeln("")
    stderr.writeln("We were not successful, closing database connection and exiting!")
finally:
    connection.close()
