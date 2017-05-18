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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.ExclusiveBufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.messages.ControlNotificationEncoder.SessionsEncoder;
import uk.co.real_logic.fix_gateway.replication.ClusterablePublication;

import java.util.List;

import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_FLAG;
import static io.aeron.protocol.DataHeaderFlyweight.END_FLAG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.fix_gateway.DebugLogger.logSbeMessage;
import static uk.co.real_logic.fix_gateway.LogTag.*;
import static uk.co.real_logic.fix_gateway.messages.ErrorDecoder.messageHeaderLength;
import static uk.co.real_logic.fix_gateway.messages.ErrorEncoder.BLOCK_LENGTH;
import static uk.co.real_logic.fix_gateway.messages.NotLeaderEncoder.libraryChannelHeaderLength;

/**
 * A proxy for publishing messages fix related messages
 */
public class GatewayPublication extends ClaimablePublication
{
    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    public static final int FRAMED_MESSAGE_SIZE = MessageHeaderEncoder.ENCODED_LENGTH + FRAME_SIZE;

    private static final byte[] NO_BYTES = {};
    private static final int CONNECT_SIZE =
        ManageConnectionEncoder.BLOCK_LENGTH + ManageConnectionDecoder.addressHeaderLength();
    private static final int HEARTBEAT_LENGTH = HEADER_LENGTH + ApplicationHeartbeatEncoder.BLOCK_LENGTH;
    private static final int LIBRARY_CONNECT_LENGTH = HEADER_LENGTH + LibraryConnectEncoder.BLOCK_LENGTH;
    private static final int DISCONNECT_LENGTH = HEADER_LENGTH + DisconnectEncoder.BLOCK_LENGTH;
    private static final int RELEASE_SESSION_LENGTH = HEADER_LENGTH + ReleaseSessionEncoder.BLOCK_LENGTH +
        ReleaseSessionEncoder.usernameHeaderLength() + ReleaseSessionEncoder.passwordHeaderLength();
    private static final int RELEASE_SESSION_REPLY_LENGTH = HEADER_LENGTH + ReleaseSessionReplyDecoder.BLOCK_LENGTH;
    private static final int REQUEST_SESSION_LENGTH = HEADER_LENGTH + RequestSessionEncoder.BLOCK_LENGTH;
    private static final int REQUEST_SESSION_REPLY_LENGTH = HEADER_LENGTH + RequestSessionReplyEncoder.BLOCK_LENGTH;
    private static final int CONNECT_FIXED_LENGTH = HEADER_LENGTH + ConnectEncoder.BLOCK_LENGTH +
        ConnectEncoder.addressHeaderLength();
    private static final int NOT_LEADER_BLOCK_LENGTH =
        NotLeaderEncoder.BLOCK_LENGTH + HEADER_LENGTH + libraryChannelHeaderLength();
    private static final int SLOW_STATUS_NOTIFICATION_LENGTH =
        HEADER_LENGTH + SlowStatusNotificationEncoder.BLOCK_LENGTH;
    private static final byte MIDDLE_FLAG = 0;

    private final SessionExistsEncoder logon = new SessionExistsEncoder();
    private final ManageConnectionEncoder manageConnection = new ManageConnectionEncoder();
    private final InitiateConnectionEncoder initiateConnection = new InitiateConnectionEncoder();
    private final RequestDisconnectEncoder requestDisconnect = new RequestDisconnectEncoder();
    private final DisconnectEncoder disconnect = new DisconnectEncoder();
    private final FixMessageEncoder fixMessage = new FixMessageEncoder();
    private final ErrorEncoder error = new ErrorEncoder();
    private final ApplicationHeartbeatEncoder applicationHeartbeat = new ApplicationHeartbeatEncoder();
    private final LibraryConnectEncoder libraryConnect = new LibraryConnectEncoder();
    private final RequestSessionEncoder requestSession = new RequestSessionEncoder();
    private final RequestSessionReplyEncoder requestSessionReply = new RequestSessionReplyEncoder();
    private final ReleaseSessionEncoder releaseSession = new ReleaseSessionEncoder();
    private final ReleaseSessionReplyEncoder releaseSessionReply = new ReleaseSessionReplyEncoder();
    private final ConnectEncoder connect = new ConnectEncoder();
    private final NewSentPositionEncoder newSentPosition = new NewSentPositionEncoder();
    private final ResetSessionIdsEncoder resetSessionIds = new ResetSessionIdsEncoder();
    private final NotLeaderEncoder notLeader = new NotLeaderEncoder();
    private final ControlNotificationEncoder controlNotification = new ControlNotificationEncoder();
    private final LibraryTimeoutEncoder libraryTimeout = new LibraryTimeoutEncoder();
    private final ResetSequenceNumberEncoder resetSequenceNumber = new ResetSequenceNumberEncoder();
    private final SlowStatusNotificationEncoder slowStatusNotification = new SlowStatusNotificationEncoder();

    private final NanoClock nanoClock;
    private final int maxPayloadLength;
    private final int maxInitialBodyLength;

    public GatewayPublication(
        final ClusterablePublication dataPublication,
        final AtomicCounter fails,
        final IdleStrategy idleStrategy,
        final NanoClock nanoClock,
        final int maxClaimAttempts)
    {
        super(maxClaimAttempts, idleStrategy, fails, dataPublication);
        this.nanoClock = nanoClock;
        this.maxPayloadLength = dataPublication.maxPayloadLength();
        this.maxInitialBodyLength = maxPayloadLength - FRAMED_MESSAGE_SIZE;
    }

    public long saveMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int libraryId,
        final int messageType,
        final long sessionId,
        final int sequenceIndex,
        final long connectionId,
        final MessageStatus status)
    {
        final ExclusiveBufferClaim bufferClaim = this.bufferClaim;
        final long timestamp = nanoClock.nanoTime();
        final int framedLength = FRAMED_MESSAGE_SIZE + srcLength;
        final boolean fragmented = framedLength > maxPayloadLength;
        final int claimLength = fragmented ? maxPayloadLength : framedLength;
        int srcFragmentLength = fragmented ? maxInitialBodyLength : srcLength;
        int srcFragmentOffset = srcOffset;

        long position = claim(claimLength);
        if (position < 0)
        {
            return position;
        }

        int offset = bufferClaim.offset();
        final MutableDirectBuffer destBuffer = bufferClaim.buffer();

        header
            .wrap(destBuffer, offset)
            .blockLength(fixMessage.sbeBlockLength())
            .templateId(fixMessage.sbeTemplateId())
            .schemaId(fixMessage.sbeSchemaId())
            .version(fixMessage.sbeSchemaVersion());

        offset += header.encodedLength();

        fixMessage
            .wrap(destBuffer, offset)
            .libraryId(libraryId)
            .messageType(messageType)
            .session(sessionId)
            .sequenceIndex(sequenceIndex)
            .connection(connectionId)
            .timestamp(timestamp)
            .status(status)
            .putBody(srcBuffer, srcFragmentOffset, srcFragmentLength);

        if (!fragmented)
        {
            bufferClaim.commit();
        }
        else
        {
            putBodyLength(srcLength, offset, destBuffer);

            bufferClaim.flags((byte) BEGIN_FLAG)
                       .commit();

            int remaining = srcLength - srcFragmentLength;
            while (remaining > 0)
            {
                srcFragmentOffset += srcFragmentLength;
                srcFragmentLength = Math.min(remaining, maxPayloadLength);

                position = claim(srcFragmentLength);
                // NB: if multiple fragments are written but never finished then
                // the message gets thrown away in re-assembly.
                if (position < 0)
                {
                    return position;
                }

                remaining -= srcFragmentLength;
                bufferClaim.buffer()
                           .putBytes(bufferClaim.offset(), srcBuffer, srcFragmentOffset, srcFragmentLength);
                bufferClaim.flags(remaining > 0 ? MIDDLE_FLAG : (byte) END_FLAG)
                           .commit();
            }
        }

        DebugLogger.log(FIX_MESSAGE, "Enqueued %s%n", srcBuffer, srcOffset, srcLength);

        return position;
    }

    private void putBodyLength(final int srcLength, final int offset, final MutableDirectBuffer destBuffer)
    {
        destBuffer.putShort(
            offset + FixMessageEncoder.BLOCK_LENGTH,
            (short) srcLength,
            LITTLE_ENDIAN);
    }

    public long saveSessionExists(
        final int libraryId,
        final long connectionId,
        final long sessionId)
    {
        return saveSessionExists(
            libraryId,
            connectionId,
            sessionId,
            SessionInfo.UNK_SESSION,
            SessionInfo.UNK_SESSION,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            LogonStatus.NEW,
            SlowStatus.NOT_SLOW);
    }

    public long saveSessionExists(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId,
        final String username,
        final String password,
        final LogonStatus logonstatus,
        final SlowStatus slowStatus)
    {
        final byte[] localCompIdBytes = bytes(localCompId);
        final byte[] localSubIdBytes = bytes(localSubId);
        final byte[] localLocationIdBytes = bytes(localLocationId);
        final byte[] remoteCompIdBytes = bytes(remoteCompId);
        final byte[] remoteSubIdBytes = bytes(remoteSubId);
        final byte[] remoteLocationIdBytes = bytes(remoteLocationId);
        final byte[] usernameBytes = bytes(username);
        final byte[] passwordBytes = bytes(password);

        final long position = claim(
            header.encodedLength() +
                SessionExistsEncoder.BLOCK_LENGTH +
                SessionExistsEncoder.localSubIdHeaderLength() * 8 +
                localCompIdBytes.length +
                localSubIdBytes.length +
                localLocationIdBytes.length +
                remoteCompIdBytes.length +
                remoteSubIdBytes.length +
                remoteLocationIdBytes.length +
                usernameBytes.length +
                passwordBytes.length);

        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        logon
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .connection(connectionId)
            .session(sessionId)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .logonStatus(logonstatus)
            .slowStatus(slowStatus)
            .putLocalCompId(localCompIdBytes, 0, localCompIdBytes.length)
            .putLocalSubId(localSubIdBytes, 0, localSubIdBytes.length)
            .putLocalLocationId(localLocationIdBytes, 0, localLocationIdBytes.length)
            .putRemoteCompId(remoteCompIdBytes, 0, remoteCompIdBytes.length)
            .putRemoteSubId(remoteSubIdBytes, 0, remoteSubIdBytes.length)
            .putRemoteLocationId(remoteLocationIdBytes, 0, remoteLocationIdBytes.length)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveManageConnection(
        final long connectionId,
        final long sessionId,
        final String address,
        final int libraryId,
        final ConnectionType type,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final SessionState sessionState,
        final int heartbeatIntervalInS,
        final long replyToId,
        final int sequenceIndex)
    {
        final byte[] addressBytes = bytes(address);

        final int length = header.encodedLength() + CONNECT_SIZE + addressBytes.length;
        final long position = claim(length);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        manageConnection
            .wrapAndApplyHeader(buffer, offset, header)
            .connection(connectionId)
            .session(sessionId)
            .libraryId(libraryId)
            .connectionType(type)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .sessionState(sessionState)
            .heartbeatIntervalInS(heartbeatIntervalInS)
            .replyToId(replyToId)
            .sequenceIndex(sequenceIndex)
            .putAddress(addressBytes, 0, addressBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        final long position = claim(DISCONNECT_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        disconnect
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .connection(connectionId)
            .reason(reason);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveConnect(final long connectionId, final String address)
    {
        final byte[] addressBytes = bytes(address);

        final long position = claim(CONNECT_FIXED_LENGTH + addressBytes.length);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        connect
            .wrapAndApplyHeader(buffer, offset, header)
            .connection(connectionId)
            .putAddress(addressBytes, 0, addressBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveResetSessionIds()
    {
        final long position = claim(HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        resetSessionIds.wrapAndApplyHeader(buffer, offset, header);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveResetSequenceNumber(final long sessionId)
    {
        final long position = claim(HEADER_LENGTH + ResetSequenceNumberEncoder.BLOCK_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        resetSequenceNumber
            .wrapAndApplyHeader(buffer, offset, header)
            .session(sessionId);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveRequestDisconnect(
        final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        final long position = claim(header.encodedLength() + RequestDisconnectEncoder.BLOCK_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        requestDisconnect
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .connection(connectionId)
            .reason(reason);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveInitiateConnection(
        final int libraryId,
        final String host,
        final int port,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String targetSubId,
        final String targetLocationId,
        final SequenceNumberType sequenceNumberType,
        final boolean resetSequenceNumber,
        final int requestedInitialSequenceNumber,
        final String username,
        final String password,
        final int heartbeatIntervalInS,
        final long correlationId)
    {
        final byte[] hostBytes = bytes(host);
        final byte[] senderCompIdBytes = bytes(senderCompId);
        final byte[] senderSubIdBytes = bytes(senderSubId);
        final byte[] senderLocationIdBytes = bytes(senderLocationId);
        final byte[] targetCompIdBytes = bytes(targetCompId);
        final byte[] targetSubIdBytes = bytes(targetSubId);
        final byte[] targetLocationIdBytes = bytes(targetLocationId);
        final byte[] usernameBytes = bytes(username);
        final byte[] passwordBytes = bytes(password);

        final long position = claim(
            header.encodedLength() +
                InitiateConnectionEncoder.BLOCK_LENGTH +
                InitiateConnectionDecoder.hostHeaderLength() * 9 +
                hostBytes.length +
                senderCompIdBytes.length +
                senderSubIdBytes.length +
                senderLocationIdBytes.length +
                targetCompIdBytes.length +
                targetSubIdBytes.length +
                targetLocationIdBytes.length +
                usernameBytes.length +
                passwordBytes.length);

        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        initiateConnection
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .port(port)
            .requestedInitialSequenceNumber(requestedInitialSequenceNumber)
            .sequenceNumberType(sequenceNumberType)
            .heartbeatIntervalInS(heartbeatIntervalInS)
            .resetSequenceNumber(resetSequenceNumber ? ResetSequenceNumber.YES : ResetSequenceNumber.NO)
            .correlationId(correlationId)
            .putHost(hostBytes, 0, hostBytes.length)
            .putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length)
            .putSenderSubId(senderSubIdBytes, 0, senderSubIdBytes.length)
            .putSenderLocationId(senderLocationIdBytes, 0, senderLocationIdBytes.length)
            .putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length)
            .putTargetSubId(targetSubIdBytes, 0, targetSubIdBytes.length)
            .putTargetLocationId(targetLocationIdBytes, 0, targetLocationIdBytes.length)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveError(
        final GatewayError errorType,
        final int libraryId,
        final long replyToId,
        final String message)
    {
        final byte[] messageBytes = bytes(message);
        final int length = header.encodedLength() + BLOCK_LENGTH + messageHeaderLength() + messageBytes.length;
        final long position = claim(length);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        error
            .wrapAndApplyHeader(buffer, offset, header)
            .errorType(errorType)
            .libraryId(libraryId)
            .replyToId(replyToId)
            .putMessage(messageBytes, 0, messageBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveApplicationHeartbeat(final int libraryId)
    {
        final long position = claim(HEARTBEAT_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        applicationHeartbeat
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId);

        bufferClaim.commit();

        logSbeMessage(APPLICATION_HEARTBEAT, buffer, bufferClaim.offset());

        return position;
    }

    public long saveLibraryConnect(final int libraryId, final long correlationId)
    {
        final long position = claim(LIBRARY_CONNECT_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        libraryConnect
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .correlationId(correlationId);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveReleaseSession(
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final SessionState state,
        final long heartbeatIntervalInMs,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String username,
        final String password)
    {
        final byte[] usernameBytes = bytes(username);
        final byte[] passwordBytes = bytes(password);

        final long position = claim(RELEASE_SESSION_LENGTH + usernameBytes.length + passwordBytes.length);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        releaseSession
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .connection(connectionId)
            .correlationId(correlationId)
            .heartbeatIntervalInMs(heartbeatIntervalInMs)
            .state(state)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveReleaseSessionReply(final int libraryId, final SessionReplyStatus status, final long replyToId)
    {
        final long position = claim(RELEASE_SESSION_REPLY_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        releaseSessionReply
            .wrapAndApplyHeader(buffer, offset, header)
            .replyToId(replyToId)
            .status(status);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveRequestSession(
        final int libraryId,
        final long sessionId,
        final long correlationId,
        final int lastReceivedSequenceNumber,
        final int sequenceIndex)
    {
        final long position = claim(REQUEST_SESSION_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        requestSession
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .sessionId(sessionId)
            .correlationId(correlationId)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .sequenceIndex(sequenceIndex);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveRequestSessionReply(final int libraryId, final SessionReplyStatus status, final long replyToId)
    {
        final long position = claim(REQUEST_SESSION_REPLY_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        requestSessionReply
            .wrapAndApplyHeader(buffer, offset, header)
            .replyToId(replyToId)
            .status(status);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveNotLeader(final int libraryId, final long replyToId, final DirectBuffer channel)
    {
        final int channelLength = (channel == null ? 0 : channel.capacity());
        final long position = claim(NOT_LEADER_BLOCK_LENGTH + channelLength);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        notLeader
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .replyToId(replyToId);

        if (channel != null)
        {
            notLeader.putLibraryChannel(channel, 0, channelLength);
        }

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveNewSentPosition(final int libraryId, final long sentPosition)
    {
        final long position = claim(NewSentPositionEncoder.BLOCK_LENGTH + HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        newSentPosition
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .position(sentPosition);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveLibraryTimeout(final int libraryId, final long connectCorrelationId)
    {
        final long position = claim(LibraryTimeoutEncoder.BLOCK_LENGTH + HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        libraryTimeout
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .connectCorrelationId(connectCorrelationId);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveControlNotification(final int libraryId, final List<SessionInfo> sessions)
    {
        final int sessionsCount = sessions.size();
        final long position = claim(
            HEADER_LENGTH +
                ControlNotificationEncoder.BLOCK_LENGTH +
                GroupSizeEncodingEncoder.ENCODED_LENGTH +
                sessionsCount * SessionsEncoder.sbeBlockLength());

        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        controlNotification
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId);

        final SessionsEncoder sessionsEncoder = controlNotification.sessionsCount(sessionsCount);
        for (int i = 0; i < sessionsCount; i++)
        {
            final long sessionId = sessions.get(i).sessionId();
            sessionsEncoder.next().sessionId(sessionId);
        }

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveSlowStatusNotification(final int libraryId, final long connectionId, final SlowStatus status)
    {
        final long position = claim(SLOW_STATUS_NOTIFICATION_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        slowStatusNotification
            .wrapAndApplyHeader(buffer, offset, header)
            .libraryId(libraryId)
            .connectionId(connectionId)
            .status(status);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public int id()
    {
        return dataPublication.id();
    }

    public long position()
    {
        return dataPublication.position();
    }

    private byte[] bytes(final String host)
    {
        if (host == null)
        {
            return NO_BYTES;
        }

        return host.getBytes(UTF_8);
    }

    public int maxPayloadLength()
    {
        return maxPayloadLength;
    }
}
