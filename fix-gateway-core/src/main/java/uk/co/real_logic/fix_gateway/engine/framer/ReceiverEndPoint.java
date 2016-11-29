/*
 * Copyright 2015-2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.engine.framer;

import org.agrona.ErrorHandler;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.Pressure;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionParser;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;
import uk.co.real_logic.fix_gateway.validation.PersistenceLevel;
import uk.co.real_logic.fix_gateway.validation.SessionPersistenceStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import static java.nio.channels.SelectionKey.OP_READ;
import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.START_OF_HEADER;
import static uk.co.real_logic.fix_gateway.engine.framer.SessionIds.DUPLICATE_SESSION;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.*;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.*;
import static uk.co.real_logic.fix_gateway.messages.SequenceNumberType.TRANSIENT;
import static uk.co.real_logic.fix_gateway.session.Session.UNKNOWN;
import static uk.co.real_logic.fix_gateway.util.AsciiBuffer.UNKNOWN_INDEX;
import static uk.co.real_logic.fix_gateway.validation.PersistenceLevel.LOCAL_ARCHIVE;
import static uk.co.real_logic.fix_gateway.validation.PersistenceLevel.REPLICATED;
import static uk.co.real_logic.fix_gateway.validation.SessionPersistenceStrategy.resetSequenceNumbersUponLogon;

/**
 * Handles incoming data from sockets.
 *
 * The receiver end point frames the TCP FIX messages into Aeron fragments.
 * It also handles backpressure coming from the Aeron stream and applies it to
 * its own TCP connections.
 */
class ReceiverEndPoint
{
    public static final char INVALID_MESSAGE_TYPE = '-';

    private static final byte BODY_LENGTH_FIELD = 9;

    private static final int COMMON_PREFIX_LENGTH = "8=FIX.4.2 ".length();
    private static final int START_OF_BODY_LENGTH = COMMON_PREFIX_LENGTH + 2;

    private static final byte CHECKSUM0 = 1;
    private static final byte CHECKSUM1 = (byte) '1';
    private static final byte CHECKSUM2 = (byte) '0';
    private static final byte CHECKSUM3 = (byte) '=';

    private static final int MIN_CHECKSUM_SIZE = " 10=".length() + 1;
    private static final int SOCKET_DISCONNECTED = -1;
    private static final int UNKNOWN_MESSAGE_TYPE = -1;
    private static final int UNKNOWN_SEQUENCE_INDEX = -2;

    private final LogonDecoder logon = new LogonDecoder();

    private final TcpChannel channel;
    private final GatewayPublication libraryPublication;
    private final GatewayPublication clusterablePublication;
    private final SessionPersistenceStrategy sessionReplicationStrategy;
    private final long connectionId;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final AtomicCounter messagesRead;
    private final Framer framer;
    private final ErrorHandler errorHandler;
    private final MutableAsciiBuffer buffer;
    private final ByteBuffer byteBuffer;
    private final LongHashSet replicatedConnectionIds;

    private GatewayPublication publication;
    private int libraryId;
    private GatewaySession gatewaySession;
    private long sessionId;
    private int usedBufferData = 0;
    private boolean hasDisconnected = false;
    private SelectionKey selectionKey;
    private boolean isPaused = false;
    private int sequenceIndex = UNKNOWN_SEQUENCE_INDEX;

    ReceiverEndPoint(
        final TcpChannel channel,
        final int bufferSize,
        final GatewayPublication clusterablePublication,
        final GatewayPublication libraryPublication,
        final SessionPersistenceStrategy sessionReplicationStrategy,
        final long connectionId,
        final long sessionId,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final AtomicCounter messagesRead,
        final Framer framer,
        final ErrorHandler errorHandler,
        final int libraryId,
        final SequenceNumberType sequenceNumberType,
        final ConnectionType connectionType,
        final LongHashSet replicatedConnectionIds)
    {
        this.channel = channel;
        this.clusterablePublication = clusterablePublication;
        this.libraryPublication = libraryPublication;
        this.sessionReplicationStrategy = sessionReplicationStrategy;
        this.connectionId = connectionId;
        this.sessionId = sessionId;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.messagesRead = messagesRead;
        this.framer = framer;
        this.errorHandler = errorHandler;
        this.libraryId = libraryId;
        this.replicatedConnectionIds = replicatedConnectionIds;

        byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        buffer = new MutableAsciiBuffer(byteBuffer);

        // Initiator sessions are persistent if the sequence numbers are expected to be persistent.
        if (connectionType == INITIATOR)
        {
            choosePublication(sequenceNumberType == TRANSIENT ? LOCAL_ARCHIVE : REPLICATED);
        }
    }

    public long connectionId()
    {
        return connectionId;
    }

    int pollForData()
    {
        if (isPaused || hasDisconnected())
        {
            return 0;
        }

        try
        {
            return readData() +
                   frameMessages();
        }
        catch (final ClosedChannelException ex)
        {
            onDisconnectDetected();
            return 1;
        }
        catch (final Exception ex)
        {
            // Regular disconnects aren't errors
            if (!ex.getMessage().contains("Connection reset by peer"))
            {
                errorHandler.onError(ex);
            }

            onDisconnectDetected();
            return 1;
        }
    }

    private int readData() throws IOException
    {
        final int dataRead = channel.read(byteBuffer);
        if (dataRead != SOCKET_DISCONNECTED)
        {
            if (dataRead > 0)
            {
                DebugLogger.log(FIX_MESSAGE, "Read     %s\n", buffer, 0, dataRead);
            }
            usedBufferData += dataRead;
        }
        else
        {
            onDisconnectDetected();
        }
        return dataRead;
    }

    private int frameMessages()
    {
        int offset = 0;
        while (true)
        {
            final int startOfBodyLength = offset + START_OF_BODY_LENGTH;
            if (usedBufferData < startOfBodyLength)
            {
                // Need more data
                break;
            }

            try
            {
                if (invalidBodyLengthTag(offset))
                {
                    invalidateMessage(offset);
                    return offset;
                }

                final int endOfBodyLength = scanEndOfBodyLength(startOfBodyLength);
                if (endOfBodyLength == UNKNOWN_INDEX)
                {
                    // Need more data
                    break;
                }

                final int startOfChecksumTag = endOfBodyLength + getBodyLength(offset, endOfBodyLength);

                final int endOfChecksumTag = startOfChecksumTag + 3;
                if (endOfChecksumTag >= usedBufferData)
                {
                    break;
                }

                if (!validateBodyLength(startOfChecksumTag))
                {
                    if (saveInvalidMessage(offset, startOfChecksumTag))
                    {
                        return offset;
                    }
                    close(INVALID_BODY_LENGTH);
                    removeEndpointFromFramer();
                    break;
                }

                final int startOfChecksumValue = startOfChecksumTag + MIN_CHECKSUM_SIZE;
                final int endOfMessage = scanEndOfMessage(startOfChecksumValue);
                if (endOfMessage == UNKNOWN_INDEX)
                {
                    // Need more data
                    break;
                }

                final int messageType = getMessageType(endOfBodyLength, endOfMessage);
                final int length = (endOfMessage + 1) - offset;
                if (checksumInvalid(endOfMessage, startOfChecksumValue, offset, startOfChecksumTag))
                {
                    if (saveInvalidChecksumMessage(offset, messageType, length))
                    {
                        return offset;
                    }
                }
                else
                {
                    if (checkSessionId(offset, length))
                    {
                        return offset;
                    }

                    messagesRead.orderedIncrement();
                    if (saveMessage(offset, messageType, length))
                    {
                        return offset;
                    }
                }

                offset += length;
            }
            catch (final IllegalArgumentException ex)
            {
                saveInvalidMessage(offset);
                return offset;
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
                break;
            }
        }

        moveRemainingDataToBufferStart(offset);
        return offset;
    }

    private boolean checksumInvalid(final int endOfMessage,
                                    final int startOfChecksumValue,
                                    final int offset,
                                    final int startOfChecksumTag)
    {
        final int expectedChecksum = buffer.getInt(startOfChecksumValue - 1, endOfMessage);
        final int computedChecksum = buffer.computeChecksum(offset, startOfChecksumTag + 1);
        return expectedChecksum != computedChecksum;
    }

    private int scanEndOfMessage(final int startOfChecksumValue)
    {
        return buffer.scan(startOfChecksumValue, usedBufferData - 1, START_OF_HEADER);
    }

    private int scanEndOfBodyLength(final int startOfBodyLength)
    {
        return buffer.scan(startOfBodyLength + 1, usedBufferData - 1, START_OF_HEADER);
    }

    private boolean checkSessionId(final int offset, final int length)
    {
        if (sessionId == UNKNOWN)
        {
            logon.decode(buffer, offset, length);
            final CompositeKey compositeKey = sessionIdStrategy.onLogon(logon.header());
            final SessionContext sessionContext = sessionIds.onLogon(compositeKey);
            sessionId = sessionContext.sessionId();
            if (sessionContext == DUPLICATE_SESSION)
            {
                final long position = libraryPublication.saveError(GatewayError.DUPLICATE_SESSION, libraryId, 0,
                    connectionId + ": Duplicate Session: " + compositeKey);
                if (Pressure.isBackPressured(position))
                {
                    moveRemainingDataToBufferStart(offset);
                    return true;
                }
                close(DisconnectReason.DUPLICATE_SESSION);
                removeEndpointFromFramer();
                return true;
            }
            else
            {
                final PersistenceLevel persistenceLevel = sessionReplicationStrategy.getPersistenceLevel(logon);
                final boolean resetSeqNum = resetSequenceNumbersUponLogon(persistenceLevel);
                final int sentSequenceNumber = sequenceNumber(sentSequenceNumberIndex, resetSeqNum, sessionId);
                final int receivedSequenceNumber = sequenceNumber(receivedSequenceNumberIndex, resetSeqNum, sessionId);
                final String username = SessionParser.username(logon);
                final String password = SessionParser.password(logon);
                if (resetSeqNum)
                {
                    sessionContext.onSequenceReset();
                }
                sequenceIndex = sessionContext.sequenceIndex();
                gatewaySession.onLogon(sessionId, compositeKey, username, password, logon.heartBtInt());
                gatewaySession.acceptorSequenceNumbers(sentSequenceNumber, receivedSequenceNumber);

                choosePublication(persistenceLevel);

                return stashIfBackpressured(offset, publication.saveLogon(
                    libraryId,
                    connectionId,
                    sessionId,
                    sentSequenceNumber,
                    receivedSequenceNumber,
                    compositeKey.senderCompId(),
                    compositeKey.senderSubId(),
                    compositeKey.senderLocationId(),
                    compositeKey.targetCompId(),
                    username,
                    password,
                    LogonStatus.NEW));
            }
        }

        return false;
    }

    private boolean stashIfBackpressured(final int offset, final long position)
    {
        final boolean backPressured = Pressure.isBackPressured(position);
        if (backPressured)
        {
            moveRemainingDataToBufferStart(offset);
        }
        return backPressured;
    }

    private boolean saveMessage(final int offset, final int messageType, final int length)
    {
        final long position = publication.saveMessage(
            buffer, offset, length, libraryId, messageType, sessionId, sequenceIndex, connectionId, OK);
        if (Pressure.isBackPressured(position))
        {
            moveRemainingDataToBufferStart(offset);
            return true;
        }
        else
        {
            gatewaySession.onMessage(buffer, offset, length, messageType, sessionId);
            return false;
        }
    }

    private int sequenceNumber(
        final SequenceNumberIndexReader sequenceNumberIndexReader,
        final boolean resetSeqNum,
        final long sessionId)
    {
        if (resetSeqNum)
        {
            return SessionInfo.UNK_SESSION;
        }

        return sequenceNumberIndexReader.lastKnownSequenceNumber(sessionId);
    }

    private boolean validateBodyLength(final int startOfChecksumTag)
    {
        return buffer.getByte(startOfChecksumTag) == CHECKSUM0
            && buffer.getByte(startOfChecksumTag + 1) == CHECKSUM1
            && buffer.getByte(startOfChecksumTag + 2) == CHECKSUM2
            && buffer.getByte(startOfChecksumTag + 3) == CHECKSUM3;
    }

    private int getMessageType(final int endOfBodyLength, final int indexOfLastByteOfMessage)
    {
        final int start = buffer.scan(endOfBodyLength, indexOfLastByteOfMessage, '=');
        if (buffer.getByte(start + 2) == START_OF_HEADER)
        {
            return buffer.getByte(start + 1);
        }
        return buffer.getMessageType(start + 1, 2);
    }

    private int getBodyLength(final int offset, final int endOfBodyLength)
    {
        return buffer.getNatural(offset + START_OF_BODY_LENGTH, endOfBodyLength);
    }

    private boolean invalidBodyLengthTag(final int offset)
    {
        try
        {
            return buffer.getDigit(offset + COMMON_PREFIX_LENGTH) != BODY_LENGTH_FIELD ||
                   buffer.getChar(offset + COMMON_PREFIX_LENGTH + 1) != '=';
        }
        catch (IllegalArgumentException e)
        {
            return false;
        }
    }

    private void moveRemainingDataToBufferStart(final int offset)
    {
        usedBufferData -= offset;
        buffer.putBytes(0, buffer, offset, usedBufferData);
        // position set to ensure that back pressure is applied to TCP when read(byteBuffer) called.
        byteBuffer.position(usedBufferData);
    }

    private void invalidateMessage(final int offset)
    {
        DebugLogger.log(FIX_MESSAGE, "%s", buffer, offset, COMMON_PREFIX_LENGTH);
    }

    private boolean saveInvalidMessage(final int offset, final int startOfChecksumTag)
    {
        return stashIfBackpressured(offset, libraryPublication.saveMessage(
            buffer,
            offset,
            libraryId,
            startOfChecksumTag,
            UNKNOWN_MESSAGE_TYPE,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID_BODYLENGTH));
    }

    private boolean saveInvalidMessage(final int offset)
    {
        final boolean backpressured = stashIfBackpressured(offset, libraryPublication.saveMessage(
            buffer,
            offset,
            usedBufferData,
            libraryId,
            INVALID_MESSAGE_TYPE,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID));

        if (!backpressured)
        {
            clearBuffer();
        }
        return backpressured;
    }

    private void clearBuffer()
    {
        moveRemainingDataToBufferStart(usedBufferData);
    }

    private boolean saveInvalidChecksumMessage(final int offset, final int messageType, final int length)
    {
        return stashIfBackpressured(offset, libraryPublication.saveMessage(
            buffer,
            offset,
            length,
            libraryId,
            messageType,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID_CHECKSUM));
    }

    public void close(final DisconnectReason reason)
    {
        closeResources();

        if (!hasDisconnected)
        {
            disconnectEndpoint(reason);
        }
    }

    private void closeResources()
    {
        try
        {
            channel.close();
            messagesRead.close();
        }
        catch (Exception e)
        {
            errorHandler.onError(e);
        }
    }

    private void removeEndpointFromFramer()
    {
        framer.onDisconnect(libraryId, connectionId, null);
    }

    private void onDisconnectDetected()
    {
        disconnectEndpoint(REMOTE_DISCONNECT);
        removeEndpointFromFramer();
    }

    void onNoLogonDisconnect()
    {
        disconnectEndpoint(NO_LOGON);
        removeEndpointFromFramer();
    }

    private void disconnectEndpoint(final DisconnectReason reason)
    {
        framer.schedule(() -> libraryPublication.saveDisconnect(libraryId, connectionId, reason));

        sessionIds.onDisconnect(sessionId);
        if (selectionKey != null)
        {
            selectionKey.cancel();
        }

        hasDisconnected = true;
    }

    boolean hasDisconnected()
    {
        return hasDisconnected;
    }

    public void register(final Selector selector) throws IOException
    {
        selectionKey = channel.register(selector, OP_READ, this);
    }

    public int libraryId()
    {
        return libraryId;
    }

    public void libraryId(final int libraryId)
    {
        this.libraryId = libraryId;
    }

    void gatewaySession(final GatewaySession gatewaySession)
    {
        this.gatewaySession = gatewaySession;
    }

    void pause()
    {
        isPaused = true;
    }

    void play()
    {
        isPaused = false;
    }

    private void choosePublication(final PersistenceLevel persistenceLevel)
    {
        if (persistenceLevel == REPLICATED)
        {
            publication = clusterablePublication;
            replicatedConnectionIds.add(connectionId);
        }
        else
        {
            publication = libraryPublication;
        }
    }
}
