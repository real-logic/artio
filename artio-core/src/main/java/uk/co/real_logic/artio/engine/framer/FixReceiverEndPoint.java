/*
 * Copyright 2015-2021 Real Logic Limited, Adaptive Financial Consulting Ltd., Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.BusinessRejectRefIdExtractor;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.dictionary.SessionConstants;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.CharFormatter;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.BitUtil.SIZE_OF_CHAR;
import static uk.co.real_logic.artio.LogTag.*;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.messages.MessageStatus.*;
import static uk.co.real_logic.artio.session.Session.UNKNOWN;
import static uk.co.real_logic.artio.util.AsciiBuffer.SEPARATOR;
import static uk.co.real_logic.artio.util.AsciiBuffer.UNKNOWN_INDEX;

/**
 * Handles incoming data from sockets.
 * <p>
 * The receiver end point frames the TCP FIX messages into Aeron fragments.
 * It also handles backpressure coming from the Aeron stream and applies it to
 * its own TCP connections.
 */
class FixReceiverEndPoint extends ReceiverEndPoint
{
    // See http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt for proxy protocol.

    private static final byte[] PROXY_V1_SIG = "PROXY ".getBytes(US_ASCII);
    private static final int PROXY_V1_SIG_LEN = PROXY_V1_SIG.length;

    private static final byte[] PROXY_V2_SIG =
        { 0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A };
    private static final int PROXY_V2_SIG_LEN = PROXY_V2_SIG.length;

    private static final int PROXY_V2_VER_CMD_OFFSET = PROXY_V2_SIG_LEN;
    private static final int PROXY_V2_VER_CMD_SIZE = 1;

    private static final byte PROXY_V2_VER = 0x20;
    private static final byte PROXY_V2_CMD_LOCAL = 0x00;
    private static final byte PROXY_V2_CMD_PROXY = 0x01;

    private static final int PROXY_V2_FAMILY_OFFSET = PROXY_V2_VER_CMD_OFFSET + PROXY_V2_VER_CMD_SIZE;
    private static final int PROXY_V2_FAMILY_SIZE = 1;

    private static final byte PROXY_V2_FAMILY_UNSPEC = 0x00;
    private static final byte PROXY_V2_FAMILY_TCP_4 = 0x11;
    private static final byte PROXY_V2_FAMILY_TCP_6 = 0x21;

    private static final int PROXY_V2_BODY_LENGTH_OFFSET = PROXY_V2_FAMILY_OFFSET + PROXY_V2_FAMILY_SIZE;
    private static final int PROXY_V2_BODY_LENGTH_SIZE = 2;

    private static final int PROXY_V2_ADDRESS_OFFSET = PROXY_V2_BODY_LENGTH_OFFSET + PROXY_V2_BODY_LENGTH_SIZE;

    /*struct { for TCP/UDP over IPv4, len = 12
        uint32_t src_addr;
        uint32_t dst_addr;
        uint16_t src_port;
        uint16_t dst_port;
    } ipv4_addr; */
    private static final int PROXY_V2_TCP4_ADDR_SIZE = 4;
    private static final int PROXY_V2_TCP4_PORT_SIZE = 2;

    private static final int PROXY_V2_TCP4_SRC_ADDR_OFFSET = PROXY_V2_ADDRESS_OFFSET;
    private static final int PROXY_V2_TCP4_DST_ADDR_OFFSET = PROXY_V2_TCP4_SRC_ADDR_OFFSET + PROXY_V2_TCP4_ADDR_SIZE;
    private static final int PROXY_V2_TCP4_SRC_PORT_OFFSET = PROXY_V2_TCP4_DST_ADDR_OFFSET + PROXY_V2_TCP4_ADDR_SIZE;
    private static final int PROXY_V2_TCP4_DST_PORT_OFFSET = PROXY_V2_TCP4_SRC_PORT_OFFSET + PROXY_V2_TCP4_PORT_SIZE;

    /*struct { for TCP/UDP over IPv6, len = 36
            uint8_t  src_addr[16];
            uint8_t  dst_addr[16];
            uint16_t src_port;
            uint16_t dst_port;
        } ipv6_addr; */
    private static final int PROXY_V2_TCP6_ADDR_SIZE = 16;
    private static final int PROXY_V2_TCP6_PORT_SIZE = 2;
    private static final int IPV6_DIGITS = 8;
    private static final int[] IPV6_LOCALHOST_DIGITS = {0, 0, 0, 0, 0, 0, 0, 1};
    private static final String IPV6_LOCALHOST = "::1:";

    private static final int PROXY_V2_TCP6_SRC_ADDR_OFFSET = PROXY_V2_ADDRESS_OFFSET;
    private static final int PROXY_V2_TCP6_DST_ADDR_OFFSET = PROXY_V2_TCP6_SRC_ADDR_OFFSET + PROXY_V2_TCP6_ADDR_SIZE;
    private static final int PROXY_V2_TCP6_SRC_PORT_OFFSET = PROXY_V2_TCP6_DST_ADDR_OFFSET + PROXY_V2_TCP6_ADDR_SIZE;
    private static final int PROXY_V2_TCP6_DST_PORT_OFFSET = PROXY_V2_TCP6_SRC_PORT_OFFSET + PROXY_V2_TCP6_PORT_SIZE;

    private static final int PROXY_V2_MIN_LENGTH = PROXY_V2_TCP6_SRC_ADDR_OFFSET;

    private static final char INVALID_MESSAGE_TYPE = '-';

    private static final byte BODY_LENGTH_FIELD = 9;
    private static final byte BEGIN_STRING_FIELD = 8;

    private static final byte CHECKSUM0 = 1;
    private static final byte CHECKSUM1 = (byte)'1';
    private static final byte CHECKSUM2 = (byte)'0';
    private static final byte CHECKSUM3 = (byte)'=';

    private static final int MIN_CHECKSUM_SIZE = " 10=".length() + 1;
    private static final int CHECKSUM_TAG_SIZE = "10=".length();
    private static final int UNKNOWN_MESSAGE_TYPE = -1;
    private static final int BREAK = -1;

    private static final int UNKNOWN_INDEX_BACKPRESSURED = -2;

    static class FixReceiverEndPointFormatters
    {
        private final CharFormatter noProxyProtocol = new CharFormatter("No proxy protocol usage for connId=%s");
        private final CharFormatter proxyV1Protocol = new CharFormatter(
            "Proxy v1 detected for connId=%s,addr=%s,line=%s");
        private final CharFormatter proxyV2Protocol = new CharFormatter(
            "Proxy v2 detected for connId=%s,addr=%s,line=%s");
    }

    private final FixContexts fixContexts;
    private final AtomicCounter messagesRead;
    private final PasswordCleaner passwordCleaner = new PasswordCleaner();
    private final BusinessRejectRefIdExtractor businessRejectRefIdExtractor = new BusinessRejectRefIdExtractor();
    private final FixGatewaySessions gatewaySessions;
    private final EpochNanoClock clock;
    private final AcceptorFixDictionaryLookup acceptorFixDictionaryLookup;
    private final FixReceiverEndPointFormatters formatters;

    private FixGatewaySession gatewaySession;
    private long sessionId;
    private int sequenceIndex;
    private boolean isPaused = false;

    private int pendingAcceptorLogonMsgOffset;
    private int pendingAcceptorLogonMsgLength;
    private long lastReadTimestampInNs;
    private String address;
    private boolean requiresProxyCheck = true;

    FixReceiverEndPoint(
        final TcpChannel channel,
        final int bufferSize,
        final GatewayPublication publication,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final FixContexts fixContexts,
        final AtomicCounter messagesRead,
        final Framer framer,
        final ErrorHandler errorHandler,
        final int libraryId,
        final FixGatewaySessions gatewaySessions,
        final EpochNanoClock clock,
        final AcceptorFixDictionaryLookup acceptorFixDictionaryLookup,
        final FixReceiverEndPointFormatters formatters,
        final int throttleWindowInMs,
        final int throttleLimitOfMessages)
    {
        super(publication, channel, connectionId, bufferSize, errorHandler, framer, libraryId,
            throttleWindowInMs, throttleLimitOfMessages);
        Objects.requireNonNull(fixContexts, "sessionContexts");
        Objects.requireNonNull(gatewaySessions, "gatewaySessions");
        Objects.requireNonNull(clock, "clock");

        this.formatters = formatters;
        this.sessionId = sessionId;
        this.sequenceIndex = sequenceIndex - 1; // Incremented on first logon
        this.fixContexts = fixContexts;
        this.messagesRead = messagesRead;
        this.gatewaySessions = gatewaySessions;
        this.clock = clock;
        this.acceptorFixDictionaryLookup = acceptorFixDictionaryLookup;

        address = channel.remoteAddr();
    }

    private int readData() throws IOException
    {
        final int dataRead = channel.read(byteBuffer);
        if (dataRead != SOCKET_DISCONNECTED)
        {
            if (dataRead > 0)
            {
                DebugLogger.log(FIX_MESSAGE_TCP, "Read     ", buffer, usedBufferData, dataRead);
            }
            usedBufferData += dataRead;
        }
        else
        {
            onDisconnectDetected();
        }

        return dataRead;
    }

    int poll()
    {
        if (isPaused || hasDisconnected())
        {
            return 0;
        }

        if (pendingAcceptorLogon != null)
        {
            return pollPendingLogon();
        }

        try
        {
            final long latestReadTimestampInNs = clock.nanoTime();
            final int bytesRead = readData();
            if (bytesRead == SOCKET_DISCONNECTED)
            {
                // Don't return the negative bytesRead below as that will indicate back-pressure
                // And trigger blocking of other receiver end points.
                return 0;
            }

            if (frameMessages(bytesRead == 0 ? lastReadTimestampInNs : latestReadTimestampInNs))
            {
                lastReadTimestampInNs = latestReadTimestampInNs;
                return bytesRead;
            }
            else
            {
                lastReadTimestampInNs = latestReadTimestampInNs;
                return -bytesRead;
            }
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

    private int pollPendingLogon()
    {
        // Retry-able under backpressure
        if (pendingAcceptorLogon.poll())
        {
            if (pendingAcceptorLogon.isAccepted())
            {
                return sendInitialLoginMessage();
            }
            else
            {
                int offset = this.pendingAcceptorLogonMsgOffset;
                int length = this.pendingAcceptorLogonMsgLength;
                DirectBuffer buffer = this.buffer;

                passwordCleaner.clean(buffer, offset, length);

                offset = 0;
                buffer = passwordCleaner.cleanedBuffer();
                length = passwordCleaner.cleanedLength();

                // No need to save this sequenceIndex update as we are at a point where a genuine session doesn't exist
                sequenceIndex++;

                final long position = publication.saveMessage(
                    buffer,
                    offset,
                    length,
                    libraryId,
                    LOGON_MESSAGE_TYPE,
                    sessionId,
                    sequenceIndex,
                    connectionId,
                    AUTH_REJECT,
                    0,
                    lastReadTimestampInNs);

                if (Pressure.isBackPressured(position))
                {
                    return 1;
                }

                DebugLogger.logFixMessage(FIX_MESSAGE, LOGON_MESSAGE_TYPE, "Auth Reject ", buffer, offset, length);

                completeDisconnect(pendingAcceptorLogon.reason());
            }
        }

        return 1;
    }

    private int sendInitialLoginMessage()
    {
        int offset = this.pendingAcceptorLogonMsgOffset;
        final int length = this.pendingAcceptorLogonMsgLength;

        // Might be paused at this point to ensure that a library has been notified of
        // the new session in initialAcceptedSessionOwner=SOLE_LIBRARY
        if (isPaused)
        {
            moveRemainingDataToBufferStart(offset);
            return offset;
        }

        final long sessionId = gatewaySession.sessionId();
        final int sequenceIndex = gatewaySession.sequenceIndex();

        if (saveMessage(offset, LOGON_MESSAGE_TYPE, length, sessionId, sequenceIndex, lastReadTimestampInNs))
        {
            // Authentication is only complete (ie this state set) when the actual logon message has been saved.
            this.sessionId = sessionId;
            this.sequenceIndex = sequenceIndex;
            pendingAcceptorLogon = null;

            framer.receiverEndPointPollingOptional(connectionId);

            // Move any data received after the logon message.
            offset += length;
            moveRemainingDataToBufferStart(offset);
            return offset;
        }
        else
        {
            return offset;
        }
    }

    boolean retryFrameMessages()
    {
        return frameMessages(lastReadTimestampInNs);
    }

    // true - no more framed messages in the buffer data to process. This could mean no more messages, or some data
    // that is an incomplete message.
    // false - needs to be retried, aka back-pressured
    private boolean frameMessages(final long readTimestampInNs)
    {
        final MutableAsciiBuffer buffer = this.buffer;
        int offset = checkProxyLine(buffer);

        while (true)
        {
            if (usedBufferData < offset + SessionConstants.MIN_MESSAGE_SIZE) // Need more data
            {
                // Need more data
                break;
            }

            try
            {
                final int startOfBodyLength = scanForBodyLength(offset, readTimestampInNs);
                if (startOfBodyLength < 0)
                {
                    return startOfBodyLength == UNKNOWN_INDEX;
                }

                final int endOfBodyLength = scanEndOfBodyLength(startOfBodyLength);
                if (endOfBodyLength == UNKNOWN_INDEX) // Need more data
                {
                    break;
                }

                final int startOfChecksumTag = endOfBodyLength + getBodyLength(startOfBodyLength, endOfBodyLength);

                final int endOfChecksumTag = startOfChecksumTag + MIN_CHECKSUM_SIZE;
                if (endOfChecksumTag >= usedBufferData)
                {
                    break;
                }

                if (!validateBodyLength(startOfChecksumTag))
                {
                    final int endOfMessage = onInvalidBodyLength(offset, startOfChecksumTag, readTimestampInNs);
                    if (endOfMessage == BREAK)
                    {
                        break;
                    }
                    return true;
                }

                final int startOfChecksumValue = startOfChecksumTag + MIN_CHECKSUM_SIZE;
                final int endOfMessage = scanEndOfMessage(startOfChecksumValue);
                if (endOfMessage == UNKNOWN_INDEX)
                {
                    break; // Need more data
                }

                final long messageType = getMessageType(endOfBodyLength, endOfMessage);
                final int length = (endOfMessage + 1) - offset;
                if (!validateChecksum(endOfMessage, startOfChecksumValue, offset, startOfChecksumTag))
                {
                    DebugLogger.logFixMessage(
                        FIX_MESSAGE, messageType, "Invalidated (checksum): ", buffer, offset, length);
                    if (saveInvalidChecksumMessage(offset, messageType, length, readTimestampInNs))
                    {
                        return false;
                    }
                }
                else
                {
                    final boolean firstMessage = messagesRead.incrementOrdered() == 0;
                    if (requiresAuthentication())
                    {
                        startAuthenticationFlow(offset, length, messageType);
                        // Actually has a logon message in it's buffer, but we return true because it's not
                        // a back-pressure scenario.
                        return true;
                    }
                    else if (messageType == LOGON_MESSAGE_TYPE)
                    {
                        onLogon(readTimestampInNs, firstMessage);
                    }

                    if (!saveMessage(offset, messageType, length, readTimestampInNs, firstMessage))
                    {
                        return false;
                    }
                }

                offset += length;
            }
            catch (final IllegalArgumentException ex)
            {
                return !invalidateMessage(offset, readTimestampInNs);
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
                break;
            }
        }

        moveRemainingDataToBufferStart(offset);
        return true;
    }

    private void onLogon(final long readTimestampInNs, final boolean firstMessage)
    {
        if (!firstMessage)
        {
            gatewaySession.onSequenceReset(readTimestampInNs);
        }
        sequenceIndex++;
    }

    private int checkProxyLine(final MutableAsciiBuffer buffer)
    {
        if (requiresProxyCheck)
        {
            final int usedBufferData = this.usedBufferData;

            if (usedBufferData > PROXY_V2_MIN_LENGTH && checkSignature(buffer, PROXY_V2_SIG))
            {
                return parseProxyV2(buffer);
            }
            else if (usedBufferData > 8 && checkSignature(buffer, PROXY_V1_SIG))
            {
                return parseProxyV1(buffer, usedBufferData);
            }
            else if (usedBufferData > PROXY_V2_MIN_LENGTH)
            {
                requiresProxyCheck = false;
                DebugLogger.log(PROXY, formatters.noProxyProtocol, connectionId);
            }
        }

        return 0;
    }

    private int parseProxyV2(final MutableAsciiBuffer buffer)
    {
        final byte verCmd = buffer.getByte(PROXY_V2_VER_CMD_OFFSET);

        if ((verCmd & 0xF0) != PROXY_V2_VER)
        {
            // Bad protocol version
            requiresProxyCheck = false;
            return 0;
        }

        switch (verCmd & 0xF)
        {
            case PROXY_V2_CMD_PROXY:
            {
                final byte family = buffer.getByte(PROXY_V2_FAMILY_OFFSET);

                switch (family)
                {
                    case PROXY_V2_FAMILY_TCP_4:
                    {
                        final int srcAddr = buffer.getInt(PROXY_V2_TCP4_SRC_ADDR_OFFSET, ByteOrder.BIG_ENDIAN);
                        final int srcPort = buffer.getChar(PROXY_V2_TCP4_SRC_PORT_OFFSET, ByteOrder.BIG_ENDIAN);

                        address = String.valueOf(0xFF & (srcAddr >> 24)) +
                            '.' +
                            (0xFF & (srcAddr >> 16)) +
                            '.' +
                            (0xFF & (srcAddr >> 8)) +
                            '.' +
                            (0xFF & srcAddr) +
                            ':' +
                            srcPort;
                        break;
                    }

                    case PROXY_V2_FAMILY_TCP_6:
                    {
                        final int[] digits = new int[IPV6_DIGITS];
                        for (int i = 0; i < IPV6_DIGITS; i++)
                        {
                            final int index = PROXY_V2_TCP6_SRC_ADDR_OFFSET + i * SIZE_OF_CHAR;
                            digits[i] = buffer.getChar(index, ByteOrder.BIG_ENDIAN);
                        }

                        final StringBuilder addressBuilder = new StringBuilder();
                        if (Arrays.equals(digits, IPV6_LOCALHOST_DIGITS))
                        {
                            addressBuilder.append(IPV6_LOCALHOST);
                        }
                        else
                        {
                            for (int i = 0; i < IPV6_DIGITS; i++)
                            {
                                addressBuilder.append(Integer.toHexString(digits[i]));
                                addressBuilder.append(':');
                            }
                        }

                        final int srcPort = buffer.getChar(PROXY_V2_TCP6_SRC_PORT_OFFSET, ByteOrder.BIG_ENDIAN);
                        addressBuilder.append(srcPort);

                        address = addressBuilder.toString();
                        break;
                    }

                    case PROXY_V2_FAMILY_UNSPEC:
                    // This default should never happen, but we'll skip things in case a malformed line is sent
                    default:
                    {
                        break;
                    }
                }
                break;
            }

            case PROXY_V2_CMD_LOCAL:
            default: // Unknown command
            {
                // Deliberately blank.
                break;
            }
        }

        final int endIndex = PROXY_V2_ADDRESS_OFFSET + proxyV2BodyLength(buffer);
        logProxyV2(buffer, endIndex);
        requiresProxyCheck = false;
        return endIndex;
    }

    private int parseProxyV1(final MutableAsciiBuffer buffer, final int usedBufferData)
    {
        int index = PROXY_V1_SIG_LEN;

        // skip protocol version: TCP4 or TCP6
        index = buffer.scan(index, usedBufferData, ' ') + 1;

        int end = buffer.scan(index, usedBufferData, ' ');
        final String sourceAddress = buffer.getAscii(index, end - index);

        // skip destination address
        index = buffer.scan(end + 1, usedBufferData, ' ') + 1;

        end = buffer.scan(index, usedBufferData, ' ');
        final String sourcePort = buffer.getAscii(index, end - index);
        address = sourceAddress + ":" + sourcePort;

        index = buffer.scan(index, usedBufferData, '\r') + 2;

        DebugLogger.log(PROXY, formatters.proxyV1Protocol, connectionId, address, buffer, 0, index);
        requiresProxyCheck = false;

        return index;
    }

    private void logProxyV2(final MutableAsciiBuffer buffer, final int endIndex)
    {
        if (DebugLogger.isEnabled(PROXY))
        {
            final byte[] bytes = new byte[endIndex];
            buffer.getBytes(0, bytes);
            DebugLogger.log(PROXY, formatters.proxyV2Protocol
                .clear()
                .with(connectionId)
                .with(address)
                .with(Arrays.toString(bytes)));
        }
    }

    private short proxyV2BodyLength(final MutableAsciiBuffer buffer)
    {
        return buffer.getShort(PROXY_V2_BODY_LENGTH_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    private boolean checkSignature(final MutableAsciiBuffer buffer, final byte[] proxyV1Sig)
    {
        for (int i = 0; i < proxyV1Sig.length; i++)
        {
            if (buffer.getByte(i) != proxyV1Sig[i])
            {
                return false;
            }
        }
        return true;
    }

    private int onInvalidBodyLength(final int offset, final int startOfChecksumTag, final long readTimestamp)
    {
        int checksumTagScanPoint = startOfChecksumTag + 1;
        while (!isStartOfChecksum(checksumTagScanPoint))
        {
            final int endOfScanPoint = checksumTagScanPoint + CHECKSUM_TAG_SIZE;
            if (endOfScanPoint >= usedBufferData)
            {
                return BREAK;
            }

            checksumTagScanPoint++;
        }

        final int endOfScanPoint = checksumTagScanPoint + CHECKSUM_TAG_SIZE;
        final int endOfMessage = buffer.scan(endOfScanPoint, usedBufferData, SEPARATOR) + 1;
        if (endOfMessage > usedBufferData)
        {
            return BREAK;
        }

        if (saveInvalidMessage(offset, endOfMessage - offset, readTimestamp))
        {
            DebugLogger.log(FIX_MESSAGE, "Invalidated (Body Length): ", buffer, offset, endOfMessage - offset);
            return offset;
        }

        moveRemainingDataToBufferStart(endOfMessage);
        return offset;
    }

    boolean requiresAuthentication()
    {
        return sessionId == UNKNOWN;
    }

    private boolean validateChecksum(
        final int endOfMessage,
        final int startOfChecksumValue,
        final int offset,
        final int startOfChecksumTag)
    {
        final int expectedChecksum = buffer.getInt(startOfChecksumValue - 1, endOfMessage);
        final int computedChecksum = buffer.computeChecksum(offset, startOfChecksumTag + 1);
        return expectedChecksum == computedChecksum;
    }

    private int scanEndOfMessage(final int startOfChecksumValue)
    {
        return buffer.scan(startOfChecksumValue, usedBufferData, START_OF_HEADER);
    }

    private int scanForBodyLength(final int offset, final long readTimestamp)
    {
        if (invalidTag(offset, BEGIN_STRING_FIELD))
        {
            return invalidateMessageUnknownIndex(offset, readTimestamp);
        }
        final int endOfCommonPrefix = scanNextField(offset + 2);
        if (endOfCommonPrefix == UNKNOWN_INDEX)
        {
            // no header end within MIN_MESSAGE_SIZE
            return invalidateMessageUnknownIndex(offset, readTimestamp);
        }
        final int startOfBodyTag = endOfCommonPrefix + 1;
        if (invalidTag(startOfBodyTag, BODY_LENGTH_FIELD))
        {
            return invalidateMessageUnknownIndex(offset, readTimestamp);
        }
        return startOfBodyTag + 2;
    }

    private int invalidateMessageUnknownIndex(final int offset, final long readTimestamp)
    {
        return invalidateMessage(offset, readTimestamp) ? UNKNOWN_INDEX_BACKPRESSURED : UNKNOWN_INDEX;
    }

    private int scanEndOfBodyLength(final int startOfBodyLength)
    {
        return buffer.scan(startOfBodyLength + 1, usedBufferData, START_OF_HEADER);
    }

    private int scanNextField(final int startScan)
    {
        return buffer.scan(startScan + 1, usedBufferData, START_OF_HEADER);
    }

    private void startAuthenticationFlow(final int offset, final int length, final long messageType)
    {
        if (sessionId != UNKNOWN)
        {
            return;
        }

        if (messageType == LOGON_MESSAGE_TYPE)
        {
            final FixDictionary fixDictionary = acceptorFixDictionaryLookup.lookup(buffer, offset, length);
            final AbstractLogonDecoder logonDecoder = fixDictionary.makeLogonDecoder();

            logonDecoder.decode(buffer, offset, length);

            pendingAcceptorLogonMsgOffset = offset;
            pendingAcceptorLogonMsgLength = length;

            pendingAcceptorLogon = gatewaySessions.authenticate(
                logonDecoder, connectionId(), gatewaySession, channel, fixDictionary, framer, address);
        }
        else
        {
            completeDisconnect(DisconnectReason.FIRST_MESSAGE_NOT_LOGON);
        }
    }

    // returns true if back-pressured
    private boolean stashIfBackPressured(final int offset, final long position)
    {
        final boolean backPressured = Pressure.isBackPressured(position);
        if (backPressured)
        {
            moveRemainingDataToBufferStart(offset);
        }

        return backPressured;
    }

    private boolean saveMessage(
        final int offset, final long messageType, final int length, final long readTimestampInNs,
        final boolean firstMessage)
    {
        if (firstMessage && messageType != LOGON_MESSAGE_TYPE)
        {
            // cover off case where the first message isn't a logon message
            gatewaySession.onSequenceReset(readTimestampInNs);
            sequenceIndex++;
        }

        return saveMessage(offset, messageType, length, sessionId, sequenceIndex, readTimestampInNs);
    }

    private boolean saveMessage(
        final int messageOffset,
        final long messageType,
        final int messageLength,
        final long sessionId,
        final int sequenceIndex,
        final long readTimestamp)
    {
        DirectBuffer buffer = this.buffer;

        if (shouldThrottle(readTimestamp))
        {
            return throttleMessage(messageOffset, messageType, messageLength, buffer);
        }
        else
        {
            int offset = messageOffset;
            int length = messageLength;

            final boolean isUserRequest = messageType == USER_REQUEST_MESSAGE_TYPE;
            if (messageType == LOGON_MESSAGE_TYPE || isUserRequest)
            {
                if (isUserRequest)
                {
                    gatewaySessions.onUserRequest(
                        buffer, offset, length, gatewaySession.fixDictionary(), connectionId, sessionId);
                }

                passwordCleaner.clean(buffer, offset, length);

                offset = 0;
                buffer = passwordCleaner.cleanedBuffer();
                length = passwordCleaner.cleanedLength();
            }

            final long position = publication.saveMessage(
                buffer,
                offset,
                length,
                libraryId,
                messageType,
                sessionId,
                sequenceIndex,
                connectionId,
                OK,
                0,
                readTimestamp);

            if (Pressure.isBackPressured(position))
            {
                moveRemainingDataToBufferStart(messageOffset);
                return false;
            }
            else
            {
                gatewaySession.onMessage(buffer, offset, length, messageType, position);
                return true;
            }
        }
    }

    private boolean throttleMessage(
        final int messageOffset, final long messageType, final int messageLength, final DirectBuffer buffer)
    {
        final BusinessRejectRefIdExtractor businessRejectRefIdExtractor = this.businessRejectRefIdExtractor;
        businessRejectRefIdExtractor.search(messageType, buffer, messageOffset, messageLength);

        final int refSeqNum = businessRejectRefIdExtractor.sequenceNumber();
        final AsciiBuffer refIdBuffer = businessRejectRefIdExtractor.buffer();
        final int refIdOffset = businessRejectRefIdExtractor.offset();
        final int refIdLength = businessRejectRefIdExtractor.length();

        final long position = publication.saveThrottleNotification(
            libraryId,
            connectionId,
            messageType,
            refSeqNum,
            sessionId,
            sequenceIndex,
            refIdBuffer, refIdOffset, refIdLength);

        if (position > 0)
        {
            return gatewaySession.onThrottleNotification(
                messageType,
                refSeqNum,
                refIdBuffer, refIdOffset, refIdLength);
        }
        else
        {
            moveRemainingDataToBufferStart(messageOffset);
            return false;
        }
    }

    private boolean validateBodyLength(final int startOfChecksumTag)
    {
        return isStartOfChecksum(startOfChecksumTag);
    }

    private boolean isStartOfChecksum(final int startOfChecksumTag)
    {
        return buffer.getByte(startOfChecksumTag) == CHECKSUM0 &&
            buffer.getByte(startOfChecksumTag + 1) == CHECKSUM1 &&
            buffer.getByte(startOfChecksumTag + 2) == CHECKSUM2 &&
            buffer.getByte(startOfChecksumTag + 3) == CHECKSUM3;
    }

    private long getMessageType(final int endOfBodyLength, final int indexOfLastByteOfMessage)
    {
        final int start = buffer.scan(endOfBodyLength, indexOfLastByteOfMessage, '=');
        if (buffer.getByte(start + 2) == START_OF_HEADER)
        {
            return buffer.getByte(start + 1);
        }
        return buffer.getMessageType(start + 1, 2);
    }

    private int getBodyLength(final int startOfBodyLength, final int endOfBodyLength)
    {
        return buffer.getNatural(startOfBodyLength, endOfBodyLength);
    }

    private boolean invalidTag(final int startOfBodyTag, final byte tagId)
    {
        try
        {
            return buffer.getDigit(startOfBodyTag) != tagId ||
                buffer.getChar(startOfBodyTag + 1) != '=';
        }
        catch (final IllegalArgumentException ex)
        {
            return false;
        }
    }

    // returns true if back-pressured
    private boolean invalidateMessage(final int offset, final long readTimestamp)
    {
        DebugLogger.log(FIX_MESSAGE, "Invalidated (IAE): ", buffer, offset, MIN_MESSAGE_SIZE);
        return saveInvalidMessage(offset, readTimestamp);
    }

    private boolean saveInvalidMessage(final int offset, final int length, final long readTimestamp)
    {
        final long position = publication.saveMessage(
            buffer,
            offset,
            length,
            libraryId,
            UNKNOWN_MESSAGE_TYPE,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID_BODYLENGTH,
            0,
            readTimestamp);

        return stashIfBackPressured(offset, position);
    }

    // returns true if back-pressured
    private boolean saveInvalidMessage(final int offset, final long readTimestamp)
    {
        final long position = publication.saveMessage(
            buffer,
            offset,
            usedBufferData - offset,
            libraryId,
            INVALID_MESSAGE_TYPE,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID,
            0,
            readTimestamp);

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

    private void moveRemainingDataToBufferStart(final int offset)
    {
        usedBufferData -= offset;
        buffer.putBytes(0, buffer, offset, usedBufferData);
        // position set to ensure that back pressure is applied to TCP when read(byteBuffer) called.
        ByteBufferUtil.position(byteBuffer, usedBufferData);
    }

    private boolean saveInvalidChecksumMessage(
        final int offset, final long messageType, final int length, final long readTimestamp)
    {
        final long position = publication.saveMessage(
            buffer,
            offset,
            length,
            libraryId,
            messageType,
            sessionId,
            sequenceIndex,
            connectionId,
            INVALID_CHECKSUM,
            0,
            readTimestamp);

        return stashIfBackPressured(offset, position);
    }

    void closeResources()
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

    void removeEndpointFromFramer()
    {
        framer.onDisconnect(libraryId, connectionId, null);
    }

    void cleanupDisconnectState(final DisconnectReason reason)
    {
        fixContexts.onDisconnect(sessionId);
        gatewaySessions.onDisconnect(sessionId, connectionId, reason);
    }

    void gatewaySession(final FixGatewaySession gatewaySession)
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

    String address()
    {
        return address;
    }

    public String toString()
    {
        return "ReceiverEndPoint: " + connectionId;
    }
}
