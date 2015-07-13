# orientoperations.nim
# Part of nim-orient by Philip Wernersbach <philip.wernersbach@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import net

import orienttypes
import orientpackets_unpack
import orientsockets_unpack
import orientpackets_pack

type
    OrientDatabase = tuple
        databaseName: OrientString not nil
        databaseType: OrientString not nil
        userName:     OrientString not nil
        userPassword: OrientString not nil

    OrientRequestDBOpen = tuple
        driverName:        OrientString not nil
        driverVersion:     OrientString not nil
        protocolVersion:   OrientShort
        clientID:          OrientString not nil
        serializationImpl: OrientString not nil
        tokenSession:      OrientBoolean
        database:          OrientDatabase

    OrientConnection* = tuple
        socket:    Socket not nil
        sessionID: OrientInt
        token:     OrientBytes not nil
        clusters:  OrientClusters

    OrientRequestCommand* = tuple
        mode:      OrientByte
        className: OrientString not nil

    OrientSQLQuery* = tuple
        text: OrientString not nil
        nonTextLimit: OrientInt
        fetchPlan: OrientString not nil
        requestCommand: OrientRequestCommand


proc send(socket: Socket, data: OrientRequestDBOpen): int =
    var dataBytes: OrientPacket

    dataBytes.packAll(OrientByte(3), OrientInt(-1), data.driverName, data.driverVersion, data.protocolVersion, data.clientID, data.serializationImpl, data.tokenSession, data.database.databaseName, data.database.databaseType, data.database.userName, data.database.userPassword)

    result = socket.send(addr(dataBytes.data[0]), dataBytes.data.len)

proc recvVerifyHeader(connection: var OrientConnection) =
    if connection.socket.unpackBoolean == false:
        raise newException(OrientCommandFailed, "REQUEST_COMMAND failed!")
    elif connection.socket.unpackInt != connection.sessionID:
        raise newException(OrientServerBug, "The session ID returned by the server does not match the connection's session ID!")

    if connection.token.len != 0:
        let token = connection.socket.unpackBytes
        if token.len != 0:
            connection.token = token

template recvResponseCommandVerifyHeaderAndReturnCollectionSize() =
    connection.recvVerifyHeader

    let synchResultType = cast[cchar](connection.socket.unpackByte)
    if synchResultType != 'l':
        raise newException(OrientDBFeatureUnsupportedInLibrary, "This library does not support synch-result-type of \"" & synchResultType & "\"!")

    var collectionSize {.inject.} = connection.socket.unpackInt

proc recvResponseCommand(connection: var OrientConnection): OrientRecords =
    recvResponseCommandVerifyHeaderAndReturnCollectionSize()
    result = newSeq[OrientRecord](collectionSize)

    collectionSize -= 1

    for i in countUp(0, collectionSize):
        result[i] = connection.socket.unpackRecord

proc recvResponseCommand(connection: var OrientConnection, onRecord: proc(record: var OrientRecord)) =
    recvResponseCommandVerifyHeaderAndReturnCollectionSize()

    collectionSize -= 1

    for i in countUp(0, collectionSize):
        var record = connection.socket.unpackRecord
        onRecord(record)

iterator recvResponseCommand(connection: var OrientConnection): OrientRecord =
    recvResponseCommandVerifyHeaderAndReturnCollectionSize()

    collectionSize -= 1

    for i in countUp(0, collectionSize):
        yield connection.socket.unpackRecord

proc send(connection: var OrientConnection, data: OrientSQLQuery): int =
    var dataBytes: OrientPacket

    # Length of command-specific data
    var commandLength = sizeof(OrientInt) + sizeof(OrientByte) * data.requestCommand.className.len + sizeof(OrientInt) + sizeof(OrientByte) * data.text.len + sizeof(OrientInt) + sizeof(OrientInt) + sizeof(OrientByte) * data.fetchPlan.len

    if connection.token.len > 0:
        commandLength += sizeof(OrientInt) + sizeof(OrientByte) * connection.token.len
        dataBytes.packAll(OrientByte(41), connection.sessionID, connection.token, data.requestCommand.mode, OrientInt(commandLength), data.requestCommand.className, data.text, data.nonTextLimit, data.fetchPlan)
    else:
        dataBytes.packAll(OrientByte(41), connection.sessionID, data.requestCommand.mode, OrientInt(commandLength), data.requestCommand.className, data.text, data.nonTextLimit, data.fetchPlan)

    # Send it off!
    result = connection.socket.send(addr(dataBytes.data[0]), dataBytes.data.len)

proc newOrientRequestCommand(mode: OrientByte, className: OrientString not nil): OrientRequestCommand =
    result = (mode: mode, className: className)

proc newOrientSQLQuery(text: OrientString not nil, nonTextLimit: OrientInt, fetchPlan: OrientString not nil, requestCommand: OrientRequestCommand): OrientSQLQuery =
    result = (text: text, nonTextLimit: nonTextLimit, fetchPlan: fetchPlan, requestCommand: requestCommand)

proc sqlQuery*(connection: var OrientConnection, query: OrientString not nil, nonTextLimit: OrientInt = -1, fetchPlan: OrientString not nil = "*:0"): OrientRecords =
    discard connection.send(newOrientSQLQuery(query, nonTextLimit, fetchPlan, newOrientRequestCommand(cast[OrientByte]('s'), "q")))
    result = connection.recvResponseCommand

proc sqlQuery*(connection: var OrientConnection, query: OrientString not nil, onRecord: proc(record: var OrientRecord), nonTextLimit: OrientInt = -1, fetchPlan: OrientString not nil = "*:0") =
    discard connection.send(newOrientSQLQuery(query, nonTextLimit, fetchPlan, newOrientRequestCommand(cast[OrientByte]('s'), "q")))
    connection.recvResponseCommand(onRecord)

iterator sqlQuery*(connection: var OrientConnection, query: OrientString not nil, nonTextLimit: OrientInt = -1, fetchPlan: OrientString not nil = "*:0") =
    discard connection.send(newOrientSQLQuery(query, nonTextLimit, fetchPlan, newOrientRequestCommand(cast[OrientByte]('s'), "q")))

    for record in connection.recvResponseCommand:
        yield record

proc newOrientDatabase*(databaseName: OrientString not nil, databaseType: OrientString not nil, userName: OrientString not nil, userPassword: OrientString not nil): OrientDatabase {.noSideEffect.} =
    result = (databaseName: databaseName, databaseType: databaseType, userName: userName, userPassword: userPassword)

proc newOrientConnection*(database: OrientDatabase, tokenSession: OrientBoolean, socket: Socket not nil): OrientConnection =
    var length: int = sizeof(OrientShort) + sizeof(OrientByte) + sizeof(OrientInt) * 3
    var responseDBOpen = newOrientPacket(length)
    let requestDBOpen: OrientRequestDBOpen = (driverName: cast[OrientString not nil]("OrientDB Nimrod Driver"), driverVersion: cast[OrientString not nil]("1.0.0"), protocolVersion: cast[OrientShort](28), clientID: cast[OrientString not nil](""), serializationImpl: cast[OrientString not nil]("ORecordSerializerBinary"), tokenSession: tokenSession, database: database)

    discard socket.send(requestDBOpen)
    discard socket.recv(responseDBOpen.buffer, length)

    # OrientDB first sends a short that has the native protocol version in it.
    # We don't use this, so discard it.
    discard responseDBOpen.unpackShort

    let status = responseDBOpen.unpackBoolean
    if status == false:
        raise newException(OrientDBOpenFailed, "Failed to open database!")

    # The Session ID in the header is bogus since we don't have a session yet.
    discard responseDBOpen.unpackInt

    # The Session ID is in the command-specific data.
    let sessionID = responseDBOpen.unpackInt
    let tokenLength = responseDBOpen.unpackInt

    if not requestDBOpen.tokenSession and (tokenLength > 0):
        raise newException(OrientServerBug, "No token requested, but server provided token anyways!")
    elif requestDBOpen.tokenSession and (tokenLength < 1):
        raise newException(OrientServerBug, "Token requested, but server did not provide token!")

    let token = socket.unpackBytes(tokenLength)
    let clusters = socket.unpackClusters

    # The OrientDB Release is useless, discard it.
    discard socket.unpackString

    result = (socket: socket, sessionID: sessionID, token: token, clusters: clusters)

proc newOrientConnection*(database: OrientDatabase, address: OrientString, tokenSession: bool, port: Port): OrientConnection =
    let socket = cast[Socket not nil](newSocket())
    socket.connect(address, port)

    result = newOrientConnection(database, tokenSession, socket)

proc close*(connection: var OrientConnection) =
    var dataBytes: OrientPacket

    if connection.token.len > 0:
        dataBytes.packAll(OrientByte(5), connection.sessionID, connection.token)
    else:
        dataBytes.packAll(OrientByte(5), connection.sessionID)

    discard connection.socket.send(addr(dataBytes.data[0]), dataBytes.data.len)
