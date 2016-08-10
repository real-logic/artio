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
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.messages.LogonStatus;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionParser;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;
import uk.co.real_logic.fix_gateway.validation.SessionReplicationStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import static io.aeron.Publication.BACK_PRESSURED;
import static java.nio.channels.SelectionKey.OP_READ;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.START_OF_HEADER;
import static uk.co.real_logic.fix_gateway.engine.framer.SessionIds.DUPLICATE_SESSION;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.*;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.*;
import static uk.co.real_logic.fix_gateway.session.Session.UNKNOWN;
import static uk.co.real_logic.fix_gateway.util.AsciiBuffer.UNKNOWN_INDEX;

/**
 * Handles incoming data from sockets.
 *
 * The receiver end point frames the TCP FIX messages into Aeron fragments.
 * It also handles backpressure coming from the Aeron stream and applies it to
 * its own TCP connections.
 */
class ReceiverEndPoint
{
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

    private final LogonDecoder logon = new LogonDecoder();

    private final TcpChannel channel;
    private final GatewayPublication libraryPublication;
    private final GatewayPublication clusterablePublication;
    private final SessionReplicationStrategy sessionReplicationStrategy;
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

    private GatewayPublication publication;
    private int libraryId;
    private final boolean resetSequenceNumbers;
    private GatewaySession gatewaySession;
    private long sessionId;
    private int usedBufferData = 0;
    private boolean hasDisconnected = false;
    private SelectionKey selectionKey;
    private boolean isPaused = false;

    ReceiverEndPoint(
        final TcpChannel channel,
        final int bufferSize,
        final GatewayPublication clusterablePublication,
        final GatewayPublication libraryPublication,
        final SessionReplicationStrategy sessionReplicationStrategy,
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
        final boolean resetSequenceNumbers)
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
        this.resetSequenceNumbers = resetSequenceNumbers;

        byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        buffer = new MutableAsciiBuffer(byteBuffer);

        // TODO: think of a cleaner way of doing this.
        // If you're initiating the session in a cluster then you need to set the publication object
        publication = resetSequenceNumbers ? libraryPublication : clusterablePublication;
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
                DebugLogger.log("Read     %s\n", buffer, 0, dataRead);
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
                    saveInvalidMessage(offset, startOfChecksumTag);
                    close(LOCAL_DISCONNECT);
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
                    saveInvalidChecksumMessage(offset, messageType, length);
                }
                else
                {
                    checkSessionId(offset, length);
                    messagesRead.orderedIncrement();
                    if (!saveMessage(offset, messageType, length))
                    {
                        moveRemainingDataToBufferStart(offset);
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

    private void saveInvalidMessage(final int offset)
    {
        // Completely unable to deal with parsing this message, bail
        libraryPublication.saveMessage(
            buffer, offset, usedBufferData, libraryId, '-', sessionId, connectionId, INVALID);
        moveRemainingDataToBufferStart(usedBufferData);
    }

    private void saveInvalidChecksumMessage(final int offset, final int messageType, final int length)
    {
        libraryPublication.saveMessage(
            buffer, offset, length, libraryId, messageType, sessionId, connectionId, INVALID_CHECKSUM);
    }

    private boolean saveMessage(final int offset, final int messageType, final int length)
    {
        final long position = publication.saveMessage(
            buffer, offset, length, libraryId, messageType, sessionId, connectionId, OK);
        if (position == BACK_PRESSURED)
        {
            return false;
        }
        else
        {
            gatewaySession.onMessage(buffer, offset, length, messageType, sessionId);
            return true;
        }
    }

    private void checkSessionId(final int offset, final int length)
    {
        if (sessionId == UNKNOWN)
        {
            logon.decode(buffer, offset, length);
            final CompositeKey compositeKey = sessionIdStrategy.onLogon(logon.header());
            sessionId = sessionIds.onLogon(compositeKey);
            if (sessionId == DUPLICATE_SESSION)
            {
                libraryPublication.saveError(GatewayError.DUPLICATE_SESSION, libraryId, 0,
                    connectionId + ": Duplicate Session: " + compositeKey);
                close(LOCAL_DISCONNECT);
            }
            else
            {
                final int sentSequenceNumber = sequenceNumber(sentSequenceNumberIndex, sessionId);
                final int receivedSequenceNumber = sequenceNumber(receivedSequenceNumberIndex, sessionId);
                final String username = SessionParser.username(logon);
                final String password = SessionParser.password(logon);
                gatewaySession.onLogon(sessionId, compositeKey, username, password, logon.heartBtInt());
                gatewaySession.sequenceNumbers(sentSequenceNumber, receivedSequenceNumber);

                if (sessionReplicationStrategy.shouldReplicate(logon))
                {
                    publication = clusterablePublication;
                }
                else
                {
                    publication = libraryPublication;
                }

                publication.saveLogon(
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
                    LogonStatus.NEW);
            }
        }
    }

    private int sequenceNumber(
        final SequenceNumberIndexReader sequenceNumberIndexReader,
        final long sessionId)
    {
        if (resetSequenceNumbers)
        {
            return SessionInfo.UNK_SESSION;
        }

        return sequenceNumberIndexReader.lastKnownSequenceNumber(sessionId);
    }

    private void saveInvalidMessage(final int offset, final int startOfChecksumTag)
    {
        libraryPublication.saveMessage(
            buffer,
            offset,
            libraryId,
            startOfChecksumTag,
            UNKNOWN_MESSAGE_TYPE,
            sessionId,
            connectionId,
            INVALID_BODYLENGTH);
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
        byteBuffer.position(usedBufferData);
    }

    private void invalidateMessage(final int offset)
    {
        DebugLogger.log("%s", buffer, offset, COMMON_PREFIX_LENGTH);
    }

    public void close(final DisconnectReason reason)
    {
        if (!hasDisconnected)
        {
            closeChannel();
            disconnectEndpoint(reason);
        }
    }

    private void closeChannel()
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
        framer.schedule(
            new Transaction(() -> libraryPublication.saveDisconnect(libraryId, connectionId, reason)));

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
}
