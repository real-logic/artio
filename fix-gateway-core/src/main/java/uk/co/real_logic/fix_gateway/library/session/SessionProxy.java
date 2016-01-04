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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.SessionRejectReason;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.AsciiFormatter;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.util.List;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static uk.co.real_logic.fix_gateway.SessionRejectReason.VALUE_IS_INCORRECT;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;

/**
 * Encapsulates sending messages relating to sessions
 */
public class SessionProxy
{
    private static final byte[] INCORRECT_BEGIN_STRING = "Incorrect BeginString".getBytes(US_ASCII);
    private static final byte[] NEGATIVE_HEARTBEAT = "HeartBtInt must not be negative".getBytes(US_ASCII);
    private static final byte[] NO_MSG_SEQ_NO = "Received message without MsgSeqNum".getBytes(US_ASCII);
    private static final int REJECT_COUNT = SessionRejectReason.values().length;
    private static final byte[][] NOT_LOGGED_ON_SESSION_REJECT_REASONS = new byte[REJECT_COUNT][];
    private static final byte[][] LOGGED_ON_SESSION_REJECT_REASONS = new byte[REJECT_COUNT][];

    static
    {
        final SessionRejectReason[] reasons = SessionRejectReason.values();
        for (int i = 0; i < REJECT_COUNT; i++)
        {
            final SessionRejectReason reason = reasons[i];
            final String formattedReason = reason.name().replace('_', ' ').toLowerCase();
            NOT_LOGGED_ON_SESSION_REJECT_REASONS[i] = String.format(
                "Tried to send a reject while not logged on: %s (field 0)",
                formattedReason
            ).getBytes(US_ASCII);

            if (reason == VALUE_IS_INCORRECT)
            {
                LOGGED_ON_SESSION_REJECT_REASONS[i] =
                    "Value is incorrect (out of range) for this tag".getBytes(US_ASCII);
            }
            else
            {
                LOGGED_ON_SESSION_REJECT_REASONS[i] = formattedReason.getBytes(US_ASCII);
            }
        }
    }

    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final LogonEncoder logon = new LogonEncoder();
    private final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();
    private final HeartbeatEncoder heartbeat = new HeartbeatEncoder();
    private final RejectEncoder reject = new RejectEncoder();
    private final TestRequestEncoder testRequest = new TestRequestEncoder();
    private final SequenceResetEncoder sequenceReset = new SequenceResetEncoder();
    private final List<HeaderEncoder> headers = asList(
        logon.header(), resendRequest.header(), logout.header(), heartbeat.header(), reject.header(),
        testRequest.header(), sequenceReset.header());

    private final AsciiFormatter lowSequenceNumber;
    private final UnsafeBuffer buffer;
    private final MutableAsciiFlyweight string;
    private final GatewayPublication gatewayPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final EpochClock clock;
    private final long connectionId;
    private final int libraryId;
    private long sessionId;

    public SessionProxy(
        final int bufferSize,
        final GatewayPublication gatewayPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final EpochClock clock,
        final long connectionId,
        final int libraryId)
    {
        this.gatewayPublication = gatewayPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.customisationStrategy = customisationStrategy;
        this.clock = clock;
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        buffer = new UnsafeBuffer(new byte[bufferSize]);
        string = new MutableAsciiFlyweight(buffer);
        lowSequenceNumber = new AsciiFormatter("MsgSeqNum too low, expecting %s but received %s");
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

    public long resendRequest(final int msgSeqNo, final int beginSeqNo, final int endSeqNo)
    {
        final HeaderEncoder header = resendRequest.header();
        header.msgSeqNum(msgSeqNo);
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
        return send(resendRequest.encode(string, 0), ResendRequestDecoder.MESSAGE_TYPE, resendRequest);
    }

    /**
     * NB: Refers to a connectionId because the session may requestDisconnect before a session id is associated
     * with it.
     *
     * @param connectionId the connection to disconnect
     */
    public long requestDisconnect(final long connectionId)
    {
        return gatewayPublication.saveRequestDisconnect(libraryId, connectionId);
    }

    public long logon(final int heartbeatInterval, final int msgSeqNo, final String username, final String password)
    {
        final HeaderEncoder header = logon.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        logon.heartBtInt(heartbeatInterval);
        if (username != null)
        {
            logon.username(username);
        }
        if (password != null)
        {
            logon.password(password);
        }
        customisationStrategy.configureLogon(logon, sessionId);

        return send(logon.encode(string, 0), LogonDecoder.MESSAGE_TYPE, logon);
    }

    public long logout(final int msgSeqNo)
    {
        return logout(msgSeqNo, null, 0);
    }

    private long logout(final int msgSeqNo, final byte[] text)
    {
        return logout(msgSeqNo, text, text.length);
    }

    private long logout(final int msgSeqNo, final byte[] text, final int length)
    {
        final HeaderEncoder header = logout.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        if (text != null)
        {
            logout.text(text, length);
        }

        customisationStrategy.configureLogout(logout, sessionId);
        return send(logout.encode(string, 0), LogoutDecoder.MESSAGE_TYPE, logout);
    }

    public void lowSequenceNumberLogout(final int msgSeqNo, final int expectedSeqNo, final int receivedSeqNo)
    {
        lowSequenceNumber
            .with(expectedSeqNo)
            .with(receivedSeqNo);

        logout(msgSeqNo, lowSequenceNumber.value(), lowSequenceNumber.length());
        lowSequenceNumber.clear();
    }

    public long incorrectBeginStringLogout(final int msgSeqNo)
    {
        return logout(msgSeqNo, INCORRECT_BEGIN_STRING);
    }

    public long negativeHeartbeatLogout(final int msgSeqNo)
    {
        return logout(msgSeqNo, NEGATIVE_HEARTBEAT);
    }

    public long receivedMessageWithoutSequenceNumber(final int msgSeqNo)
    {
        return logout(msgSeqNo, NO_MSG_SEQ_NO);
    }

    public long rejectWhilstNotLoggedOn(final int msgSeqNo, final SessionRejectReason reason)
    {
        return logout(msgSeqNo, NOT_LOGGED_ON_SESSION_REJECT_REASONS[reason.ordinal()]);
    }

    public long heartbeat(final int msgSeqNo)
    {
        return heartbeat(null, 0, msgSeqNo);
    }

    public long heartbeat(final char[] testReqId, final int testReqIdLength, final int msgSeqNo)
    {
        final HeaderEncoder header = heartbeat.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        if (testReqId != null)
        {
            heartbeat.testReqID(testReqId, testReqIdLength);
        }
        else
        {
            heartbeat.resetTestReqID();
        }

        return send(heartbeat.encode(string, 0), HeartbeatDecoder.MESSAGE_TYPE, heartbeat);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final int refTagId,
        final byte[] refMsgType,
        final int refMsgTypeLength,
        final SessionRejectReason reason)
    {
        reject.refTagID(refTagId);
        return reject(msgSeqNo, refSeqNum, refMsgType, refMsgTypeLength, reason);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final byte[] refMsgType,
        final int refMsgTypeLength,
        final SessionRejectReason reason)
    {
        final int rejectReason = reason.representation();

        reject.refMsgType(refMsgType, refMsgTypeLength);
        reject.text(LOGGED_ON_SESSION_REJECT_REASONS[rejectReason]);

        return sendReject(msgSeqNo, refSeqNum, rejectReason);
    }

    public void reject(
        final int msgSeqNo,
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason)
    {
        reject.refTagID(refTagId);
        reject(msgSeqNo, refSeqNum, refMsgType, refMsgTypeLength, rejectReason);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason)
    {
        reject.refMsgType(refMsgType, refMsgTypeLength);
        reject.text(LOGGED_ON_SESSION_REJECT_REASONS[rejectReason]);

        return sendReject(msgSeqNo, refSeqNum, rejectReason);
    }

    private long sendReject(final int msgSeqNo, final int refSeqNum, final int rejectReason)
    {
        final HeaderEncoder header = reject.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        reject.refSeqNum(refSeqNum);
        reject.sessionRejectReason(rejectReason);

        return send(reject.encode(string, 0), RejectDecoder.MESSAGE_TYPE, reject);
    }

    public long testRequest(final int msgSeqNo, final CharSequence testReqID)
    {
        final HeaderEncoder header = testRequest.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        testRequest.testReqID(testReqID);

        return send(testRequest.encode(string, 0), TestRequestDecoder.MESSAGE_TYPE, testRequest);
    }

    public long sequenceReset(final int msgSeqNo, final int newSeqNo)
    {
        final HeaderEncoder header = sequenceReset.header();
        setupHeader(header);
        header.msgSeqNum(msgSeqNo);

        sequenceReset.newSeqNo(newSeqNo);

        return send(sequenceReset.encode(string, 0), SequenceResetDecoder.MESSAGE_TYPE, sequenceReset);
    }

    private void setupHeader(final HeaderEncoder header)
    {
        timestampEncoder.encode(clock.time());
        header.sendingTime(timestampEncoder.buffer());
    }

    private long send(final int length, final int messageType, final Encoder encoder)
    {
        final long position = gatewayPublication.saveMessage(
            buffer, 0, length, libraryId, messageType, sessionId, connectionId, OK);
        encoder.reset();
        return position;
    }
}
