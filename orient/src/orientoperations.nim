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
        clientID:          OrientString
        serializationImpl: OrientString
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
    var length = sizeof(OrientByte) + sizeof(OrientInt) + sizeof(OrientInt) * 8 + sizeof(OrientShort) + sizeof(OrientBoolean) + data.driverName.len + data.driverVersion.len + data.database.databaseName.len + data.database.databaseType.len + data.database.userName.len + data.database.userPassword.len
    var dataBytes: OrientPacket

    if data.clientID != nil:
        length += data.clientID.len

    if data.serializationImpl != nil:
        length += data.serializationImpl.len

    dataBytes = newOrientPacket(length)
    discard dataBytes.pack(cast[OrientByte](3)).pack(cast[OrientInt](-1)).pack(data.driverName).pack(data.driverVersion).pack(data.protocolVersion).pack(data.clientID).pack(data.serializationImpl).pack(data.tokenSession).pack(data.database.databaseName).pack(data.database.databaseType).pack(data.database.userName).pack(data.database.userPassword)

    return socket.send(addr(dataBytes.data[0]), length)

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
    # Length of command-specific data
    let commandLength = sizeof(OrientInt) + sizeof(OrientByte) * data.requestCommand.className.len + sizeof(OrientInt) + sizeof(OrientByte) * data.text.len + sizeof(OrientInt) + sizeof(OrientInt) + sizeof(OrientByte) * data.fetchPlan.len

    # Length of request header plus command-specific data
    var length = sizeof(OrientByte) + sizeof(OrientInt) + sizeof(OrientByte) + sizeof(OrientInt) + commandLength

    # Make room for the optional token, if needed.
    if connection.token.len > 0:
        length += sizeof(OrientInt) + sizeof(OrientByte) * connection.token.len

    # Allocate our packet.
    var dataBytes = newOrientPacket(length)

    # Pack the request header.
    discard dataBytes.pack(cast[OrientByte](41)).pack(connection.sessionID)

    # Pack the optional token, if needed.
    if connection.token.len > 0:
        discard dataBytes.pack(connection.token)

    # Pack the command-specific data.
    discard dataBytes.pack(data.requestCommand.mode).pack(cast[OrientInt](commandLength)).pack(data.requestCommand.className)
    discard dataBytes.pack(data.text).pack(data.nonTextLimit).pack(data.fetchPlan)

    # Send it off!
    return connection.socket.send(addr(dataBytes.data[0]), length)

proc newOrientRequestCommand(mode: OrientByte, className: OrientString not nil): OrientRequestCommand =
    return (mode: mode, className: className)

proc newOrientSQLQuery(text: OrientString not nil, nonTextLimit: OrientInt, fetchPlan: OrientString not nil, requestCommand: OrientRequestCommand): OrientSQLQuery =
    return (text: text, nonTextLimit: nonTextLimit, fetchPlan: fetchPlan, requestCommand: requestCommand)

proc sqlQuery*(connection: var OrientConnection, query: OrientString not nil, nonTextLimit: OrientInt, fetchPlan: OrientString not nil): OrientRecords =
    discard connection.send(newOrientSQLQuery(query, nonTextLimit, fetchPlan, newOrientRequestCommand(cast[OrientByte]('s'), "q")))
    return connection.recvResponseCommand

proc sqlQuery*(connection: var OrientConnection, query: OrientString not nil, nonTextLimit: OrientInt, fetchPlan: OrientString not nil, onRecord: proc(record: var OrientRecord)) =
    discard connection.send(newOrientSQLQuery(query, nonTextLimit, fetchPlan, newOrientRequestCommand(cast[OrientByte]('s'), "q")))
    connection.recvResponseCommand(onRecord)

iterator sqlQuery*(connection: var OrientConnection, query: OrientString not nil, nonTextLimit: OrientInt, fetchPlan: OrientString not nil) =
    discard connection.send(newOrientSQLQuery(query, nonTextLimit, fetchPlan, newOrientRequestCommand(cast[OrientByte]('s'), "q")))

    for record in connection.recvResponseCommand:
        yield record

proc newOrientDatabase*(databaseName: OrientString not nil, databaseType: OrientString not nil, userName: OrientString not nil, userPassword: OrientString not nil): OrientDatabase {.noSideEffect.} =
    return (databaseName: databaseName, databaseType: databaseType, userName: userName, userPassword: userPassword)

proc newOrientConnection*(database: OrientDatabase, tokenSession: OrientBoolean, socket: Socket not nil): OrientConnection =
    var length: int = sizeof(OrientShort) + sizeof(OrientByte) + sizeof(OrientInt) * 3
    var responseDBOpen = newOrientPacket(length)
    let requestDBOpen: OrientRequestDBOpen = (driverName: cast[OrientString not nil]("OrientDB Nimrod Driver"), driverVersion: cast[OrientString not nil]("1.0.0"), protocolVersion: cast[OrientShort](28), clientID: nil, serializationImpl: "ORecordSerializerBinary", tokenSession: tokenSession, database: database)

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

    return (socket: socket, sessionID: sessionID, token: token, clusters: clusters)

proc newOrientConnection*(database: OrientDatabase, address: OrientString, tokenSession: bool, port: Port): OrientConnection =
    let socket = cast[Socket not nil](newSocket())
    socket.connect(address, port)

    return newOrientConnection(database, tokenSession, socket)

proc close*(connection: var OrientConnection) =
    let commandLength = 0
    var length = sizeof(OrientByte) + sizeof(OrientInt) + sizeof(OrientByte) + sizeof(OrientInt) + commandLength

    if connection.token.len > 0:
        length += sizeof(OrientInt) + sizeof(OrientByte) * connection.token.len

    var dataBytes = newOrientPacket(length)

    discard dataBytes.pack(cast[OrientByte](5)).pack(connection.sessionID)

    if connection.token.len > 0:
        discard dataBytes.pack(connection.token)

    discard connection.socket.send(addr(dataBytes.data[0]), length)
