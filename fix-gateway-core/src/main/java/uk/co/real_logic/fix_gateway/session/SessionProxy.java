/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.session;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.SessionRejectReason;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.AsciiFormatter;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.util.List;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;

/**
 * Encapsulates sending messages relating to sessions
 */
public class SessionProxy
{
    private static final byte[] INCORRECT_BEGIN_STRING = "Incorrect BeginString".getBytes(US_ASCII);
    private static final byte[] NEGATIVE_HEARTBEAT = "HeartBtInt must not be negative".getBytes(US_ASCII);
    private static final byte[] NO_MSG_SEQ_NO = "Received message without MsgSeqNum".getBytes(US_ASCII);
    private static final byte[][] SESSION_REJECT_REASONS = new byte[SessionRejectReason.values().length][];

    static
    {
        final SessionRejectReason[] reasons = SessionRejectReason.values();
        for (int i = 0; i < reasons.length; i++)
        {
            final SessionRejectReason reason = reasons[i];
            SESSION_REJECT_REASONS[i] = String.format(
                // TODO: try to understand the formatting here
                "Tried to send a reject while not logged on: %s (field 0)",
                reason.name().replace('_', ' ').toLowerCase()
            ).getBytes(US_ASCII);
        }
    }

    private final LogonEncoder logon = new LogonEncoder();
    private final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();
    private final HeartbeatEncoder heartbeat = new HeartbeatEncoder();
    private final RejectEncoder reject = new RejectEncoder();
    private final List<HeaderEncoder> headers = asList(
        logon.header(), resendRequest.header(), logout.header(), heartbeat.header(), reject.header());

    private final AsciiFormatter logSequenceNumber;
    private final UnsafeBuffer buffer;
    private final MutableAsciiFlyweight string;
    private final GatewayPublication gatewayPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private long sessionId;

    public SessionProxy(
        final int bufferSize,
        final GatewayPublication gatewayPublication,
        final SessionIdStrategy sessionIdStrategy)
    {
        this.gatewayPublication = gatewayPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        buffer = new UnsafeBuffer(new byte[bufferSize]);
        string = new MutableAsciiFlyweight(buffer);
        logSequenceNumber = new AsciiFormatter("MsgSeqNum too low, expecting %s but received %s");
    }

    public SessionProxy setupSession(final long sessionId, final Object sessionKey)
    {
        this.sessionId = sessionId;
        for (final HeaderEncoder header : headers)
        {
            sessionIdStrategy.setupSession(sessionKey, header);
        }
        return this;
    }

    public void resendRequest(final int msgSeqNo, final int beginSeqNo, final int endSeqNo)
    {
        final HeaderEncoder header = resendRequest.header();
        header.msgSeqNum(msgSeqNo);
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
        send(resendRequest.encode(string, 0), ResendRequestDecoder.MESSAGE_TYPE);
    }

    /**
     * NB: Refers to a connectionId because the session may disconnect before a session id is associated
     * with it.
     *
     * @param connectionId
     */
    public void disconnect(final long connectionId)
    {
        gatewayPublication.saveDisconnect(connectionId);
    }

    public void logon(final int heartbeatInterval, final int msgSeqNo)
    {
        final HeaderEncoder header = logon.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        logon.heartBtInt(heartbeatInterval);
        send(logon.encode(string, 0), LogonDecoder.MESSAGE_TYPE);
    }

    // TODO: remove this method once everything has messages
    public void logout(final int msgSeqNo)
    {
        logout(msgSeqNo, null, 0);
    }

    private void logout(final int msgSeqNo, final byte[] text)
    {
        logout(msgSeqNo, text, text.length);
    }

    private void logout(final int msgSeqNo, final byte[] text, final int length)
    {
        final HeaderEncoder header = logout.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        if (text != null)
        {
            logout.text(text, length);
        }

        send(logout.encode(string, 0), LogoutDecoder.MESSAGE_TYPE);
    }

    public void lowSequenceNumberLogout(final int msgSeqNo, final int expectedSeqNo, final int receivedSeqNo)
    {
        logSequenceNumber
            .with(expectedSeqNo)
            .with(receivedSeqNo);

        logout(msgSeqNo, logSequenceNumber.value(), logSequenceNumber.length());
    }

    public void incorrectBeginStringLogout(final int msgSeqNo)
    {
        logout(msgSeqNo, INCORRECT_BEGIN_STRING);
    }

    public void negativeHeartbeatLogout(final int msgSeqNo)
    {
        logout(msgSeqNo, NEGATIVE_HEARTBEAT);
    }

    public void receivedMessageWithoutSequenceNumber(final int msgSeqNo)
    {
        logout(msgSeqNo, NO_MSG_SEQ_NO);
    }

    public void rejectWhilstNotLoggedOn(final int msgSeqNo, final SessionRejectReason reason)
    {
        logout(msgSeqNo, SESSION_REJECT_REASONS[reason.ordinal()]);
    }

    public void heartbeat(final int msgSeqNo)
    {
        heartbeat(null, 0, msgSeqNo);
    }

    public void heartbeat(final char[] testReqId, final int testReqIdLength, final int msgSeqNo)
    {
        final HeaderEncoder header = heartbeat.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        if (testReqId != null)
        {
            heartbeat.testReqID(testReqId, testReqIdLength);
        }
        send(heartbeat.encode(string, 0), HeartbeatDecoder.MESSAGE_TYPE);
    }

    public void reject(final int msgSeqNo, final int refSeqNum)
    {
        final HeaderEncoder header = reject.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        reject.refSeqNum(refSeqNum);
        // TODO: decide on other ref fields
        send(reject.encode(string, 0), RejectDecoder.MESSAGE_TYPE);
    }

    private void setupHeader(final HeaderEncoder header)
    {
        header.sendingTime(System.currentTimeMillis());
    }

    private void send(final int length, final int messageType)
    {
        gatewayPublication.saveMessage(buffer, 0, length, this.sessionId, messageType);
    }
}
