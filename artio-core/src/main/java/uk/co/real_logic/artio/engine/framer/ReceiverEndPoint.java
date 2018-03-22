/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.PersistenceLevel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;

import static java.nio.channels.SelectionKey.OP_READ;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.artio.dictionary.StandardFixConstants.START_OF_HEADER;
import static uk.co.real_logic.artio.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.artio.messages.DisconnectReason.*;
import static uk.co.real_logic.artio.messages.MessageStatus.*;
import static uk.co.real_logic.artio.messages.SequenceNumberType.TRANSIENT;
import static uk.co.real_logic.artio.session.Session.UNKNOWN;
import static uk.co.real_logic.artio.util.AsciiBuffer.UNKNOWN_INDEX;
import static uk.co.real_logic.artio.validation.PersistenceLevel.LOCAL_ARCHIVE;
import static uk.co.real_logic.artio.validation.PersistenceLevel.REPLICATED;

/**
 * Handles incoming data from sockets.
 * <p>
 * The receiver end point frames the TCP FIX messages into Aeron fragments.
 * It also handles backpressure coming from the Aeron stream and applies it to
 * its own TCP connections.
 */
class ReceiverEndPoint
{
    private static final char INVALID_MESSAGE_TYPE = '-';

    private static final byte BODY_LENGTH_FIELD = 9;

    private final int commonPrefixLength;
    private final int startOfBodyLenght;

    private static final byte CHECKSUM0 = 1;
    private static final byte CHECKSUM1 = (byte)'1';
    private static final byte CHECKSUM2 = (byte)'0';
    private static final byte CHECKSUM3 = (byte)'=';

    private static final int MIN_CHECKSUM_SIZE = " 10=".length() + 1;
    private static final int SOCKET_DISCONNECTED = -1;
    private static final int UNKNOWN_MESSAGE_TYPE = -1;

    private final LogonDecoder logon = new LogonDecoder();

    private final TcpChannel channel;
    private final GatewayPublication libraryPublication;
    private final GatewayPublication clusterablePublication;
    private final long connectionId;
    private final SessionContexts sessionContexts;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final AtomicCounter messagesRead;
    private final Framer framer;
    private final ErrorHandler errorHandler;
    private final MutableAsciiBuffer buffer;
    private final ByteBuffer byteBuffer;
    private final LongHashSet replicatedConnectionIds;
    private final GatewaySessions gatewaySessions;

    private GatewayPublication publication;
    private int libraryId;
    private GatewaySession gatewaySession;
    private long sessionId;
    private int sequenceIndex;
    private int usedBufferData = 0;
    private boolean hasDisconnected = false;
    private SelectionKey selectionKey;
    private boolean isPaused = false;

    ReceiverEndPoint(
        final TcpChannel channel,
        final int bufferSize,
        final GatewayPublication libraryPublication,
        final GatewayPublication clusterablePublication,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final SessionContexts sessionContexts,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final AtomicCounter messagesRead,
        final Framer framer,
        final ErrorHandler errorHandler,
        final int libraryId,
        final SequenceNumberType sequenceNumberType,
        final ConnectionType connectionType,
        final LongHashSet replicatedConnectionIds,
        final GatewaySessions gatewaySessions,
        final int commonPrefixLength)
    {
        Objects.requireNonNull(clusterablePublication, "clusterablePublication");
        Objects.requireNonNull(libraryPublication, "libraryPublication");
        Objects.requireNonNull(sessionContexts, "sessionContexts");
        Objects.requireNonNull(gatewaySessions, "gatewaySessions");

        this.channel = channel;
        this.clusterablePublication = clusterablePublication;
        this.libraryPublication = libraryPublication;
        this.connectionId = connectionId;
        this.sessionId = sessionId;
        this.sequenceIndex = sequenceIndex;
        this.sessionContexts = sessionContexts;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.messagesRead = messagesRead;
        this.framer = framer;
        this.errorHandler = errorHandler;
        this.libraryId = libraryId;
        this.replicatedConnectionIds = replicatedConnectionIds;
        this.gatewaySessions = gatewaySessions;

        byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        buffer = new MutableAsciiBuffer(byteBuffer);
        this.commonPrefixLength = commonPrefixLength;
        this.startOfBodyLenght = this.commonPrefixLength + 2;
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
            return readData() + frameMessages();
        }
        catch (final ClosedChannelException ex)
        {
            onDisconnectDetected();
            return 1;
        }
        catch (final Exception ex)
        {
            // Regular disconnects aren't errors
            if (!Exceptions.isJustDisconnect(ex))
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
                DebugLogger.log(FIX_MESSAGE, "Read     %s%n", buffer, 0, dataRead);
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
            final int startOfBodyLength = offset + startOfBodyLenght;
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

                // TODO(Nick): We already scan for the message type so we can check for logon messages here?
                final int messageType = getMessageType(endOfBodyLength, endOfMessage);
                final int length = (endOfMessage + 1) - offset;
                if (validateChecksum(endOfMessage, startOfChecksumValue, offset, startOfChecksumTag))
                {
                    if (saveInvalidChecksumMessage(offset, messageType, length))
                    {
                        return offset;
                    }
                }
                else
                {
                    if (UNKNOWN == sessionId && checkSessionId(offset, length))
                    {
                        return offset;
                    }

                    messagesRead.incrementOrdered();
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

    private boolean validateChecksum(
        final int endOfMessage,
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
        if (sessionId != UNKNOWN)
        {
            return false;
        }

        logon.decode(buffer, offset, length);

        final AuthenticationResult authResult = gatewaySessions.authenticateAndInitiate(
            logon,
            connectionId(),
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex,
            gatewaySession);

        if (authResult.isDuplicateSession())
        {
            close(DisconnectReason.DUPLICATE_SESSION);
            removeEndpointFromFramer();

            return true;
        }

        if (!authResult.isValid())
        {
            onInvalidLogon();
            return true;
        }

        sessionId = gatewaySession.sessionId();
        sequenceIndex = gatewaySession.sequenceIndex();

        choosePublication(gatewaySession.persistenceLevel());

        return false;
    }

    private boolean stashIfBackPressured(final int offset, final long position)
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
        final long position = publication.saveMessage(buffer,
            offset,
            length,
            libraryId,
            messageType,
            sessionId,
            sequenceIndex,
            connectionId,
            OK);

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

    private boolean validateBodyLength(final int startOfChecksumTag)
    {
        return buffer.getByte(startOfChecksumTag) == CHECKSUM0 &&
            buffer.getByte(startOfChecksumTag + 1) == CHECKSUM1 &&
            buffer.getByte(startOfChecksumTag + 2) == CHECKSUM2 &&
            buffer.getByte(startOfChecksumTag + 3) == CHECKSUM3;
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
        return buffer.getNatural(offset + startOfBodyLenght, endOfBodyLength);
    }

    private boolean invalidBodyLengthTag(final int offset)
    {
        try
        {
            return buffer.getDigit(offset + commonPrefixLength) != BODY_LENGTH_FIELD ||
                   buffer.getChar(offset + commonPrefixLength + 1) != '=';
        }
        catch (final IllegalArgumentException ex)
        {
            return false;
        }
    }

    private void moveRemainingDataToBufferStart(final int offset)
    {
        usedBufferData -= offset;
        buffer.putBytes(0, buffer, offset, usedBufferData);
        // position set to ensure that back pressure is applied to TCP when read(byteBuffer) called.
        ByteBufferUtil.position(byteBuffer, usedBufferData);
    }

    private void invalidateMessage(final int offset)
    {
        DebugLogger.log(FIX_MESSAGE, "%s", buffer, offset, commonPrefixLength);
    }

    private boolean saveInvalidMessage(final int offset, final int startOfChecksumTag)
    {
        final long position = libraryPublication.saveMessage(
            buffer,
            offset,
            libraryId,
            startOfChecksumTag,
            UNKNOWN_MESSAGE_TYPE,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID_BODYLENGTH);

        return stashIfBackPressured(offset, position);
    }

    private boolean saveInvalidMessage(final int offset)
    {
        final long position = libraryPublication.saveMessage(buffer,
            offset,
            usedBufferData,
            libraryId,
            INVALID_MESSAGE_TYPE,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID);

        final boolean backPressured = stashIfBackPressured(offset, position);

        if (!backPressured)
        {
            clearBuffer();
        }

        return backPressured;
    }

    private void clearBuffer()
    {
        moveRemainingDataToBufferStart(usedBufferData);
    }

    private boolean saveInvalidChecksumMessage(final int offset, final int messageType, final int length)
    {
        final long position = libraryPublication.saveMessage(buffer,
            offset,
            length,
            libraryId,
            messageType,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID_CHECKSUM);

        return stashIfBackPressured(offset, position);
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
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
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

    private void onInvalidLogon()
    {
        disconnectEndpoint(DisconnectReason.FAILED_AUTHENTICATION);
        removeEndpointFromFramer();
    }

    private void disconnectEndpoint(final DisconnectReason reason)
    {
        framer.schedule(() -> libraryPublication.saveDisconnect(libraryId, connectionId, reason));

        sessionContexts.onDisconnect(sessionId);
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
