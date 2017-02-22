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
package uk.co.real_logic.fix_gateway.session;

import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.fields.RejectReason;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.AsciiFormatter;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.util.List;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.fix_gateway.fields.RejectReason.VALUE_IS_INCORRECT;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.session.Session.LIBRARY_DISCONNECTED;

/**
 * Encapsulates sending messages relating to sessions
 */
public class SessionProxy
{
    private static final byte[] INCORRECT_BEGIN_STRING = "Incorrect BeginString".getBytes(US_ASCII);
    private static final byte[] NEGATIVE_HEARTBEAT = "HeartBtInt must not be negative".getBytes(US_ASCII);
    private static final byte[] NO_MSG_SEQ_NO = "Received message without MsgSeqNum".getBytes(US_ASCII);
    private static final int REJECT_COUNT = RejectReason.values().length;
    private static final byte[][] NOT_LOGGED_ON_SESSION_REJECT_REASONS = new byte[REJECT_COUNT][];
    private static final byte[][] LOGGED_ON_SESSION_REJECT_REASONS = new byte[REJECT_COUNT][];

    static
    {
        final RejectReason[] reasons = RejectReason.values();
        for (int i = 0; i < REJECT_COUNT; i++)
        {
            final RejectReason reason = reasons[i];
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
    private final MutableAsciiBuffer string;
    private final GatewayPublication gatewayPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final EpochClock clock;
    private final long connectionId;
    private final int libraryId;
    private long sessionId;
    private boolean libraryConnected = true;

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
        string = new MutableAsciiBuffer(buffer);
        lowSequenceNumber = new AsciiFormatter("MsgSeqNum too low, expecting %s but received %s");
        timestampEncoder.initialise(clock.time());
    }

    public SessionProxy setupSession(final long sessionId, final CompositeKey sessionKey)
    {
        requireNonNull(sessionKey, "sessionKey");

        this.sessionId = sessionId;
        for (final HeaderEncoder header : headers)
        {
            sessionIdStrategy.setupSession(sessionKey, header);
        }

        return this;
    }

    long resendRequest(final int msgSeqNo, final int beginSeqNo, final int endSeqNo, final int sequenceIndex)
    {
        final HeaderEncoder header = resendRequest.header();
        setupHeader(header, msgSeqNo);
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
        final int length = resendRequest.encode(string, 0);
        return send(length, ResendRequestDecoder.MESSAGE_TYPE, sequenceIndex, resendRequest);
    }

    public long requestDisconnect(final long connectionId, final DisconnectReason reason)
    {
        return gatewayPublication.saveRequestDisconnect(libraryId, connectionId, reason);
    }

    public long logon(
        final int heartbeatIntervalInS,
        final int msgSeqNo,
        final String username,
        final String password,
        final boolean resetSeqNumFlag,
        final int sequenceIndex)
    {
        final HeaderEncoder header = logon.header();
        setupHeader(header, msgSeqNo);

        logon
            .heartBtInt(heartbeatIntervalInS)
            .resetSeqNumFlag(resetSeqNumFlag)
            .encryptMethod(0); // may have been previously reset

        if (!nullOrEmpty(username))
        {
            logon.username(username);
        }
        if (!nullOrEmpty(password))
        {
            logon.password(password);
        }
        customisationStrategy.configureLogon(logon, sessionId);

        final int length = logon.encode(string, 0);
        return send(length, LogonDecoder.MESSAGE_TYPE, sequenceIndex, logon);
    }

    private boolean nullOrEmpty(final String string)
    {
        return string == null || string.length() == 0;
    }

    public long logout(final int msgSeqNo, final int sequenceIndex)
    {
        return logout(msgSeqNo, null, 0, sequenceIndex);
    }

    private long logout(final int msgSeqNo, final byte[] text, final int sequenceIndex)
    {
        return logout(msgSeqNo, text, text.length, sequenceIndex);
    }

    private long logout(final int msgSeqNo, final byte[] text, final int length, final int sequenceIndex)
    {
        final HeaderEncoder header = logout.header();
        setupHeader(header, msgSeqNo);

        if (text != null)
        {
            logout.text(text, length);
        }

        customisationStrategy.configureLogout(logout, sessionId);
        return send(logout.encode(string, 0), LogoutDecoder.MESSAGE_TYPE, sequenceIndex, logout);
    }

    public long lowSequenceNumberLogout(
        final int msgSeqNo,
        final int expectedSeqNo,
        final int receivedSeqNo,
        final int sequenceIndex)
    {
        lowSequenceNumber
            .with(expectedSeqNo)
            .with(receivedSeqNo);

        final long position = logout(msgSeqNo, lowSequenceNumber.value(), lowSequenceNumber.length(), sequenceIndex);
        lowSequenceNumber.clear();

        return position;
    }

    public long incorrectBeginStringLogout(final int msgSeqNo, final int sequenceIndex)
    {
        return logout(msgSeqNo, INCORRECT_BEGIN_STRING, sequenceIndex);
    }

    public long negativeHeartbeatLogout(final int msgSeqNo, final int sequenceIndex)
    {
        return logout(msgSeqNo, NEGATIVE_HEARTBEAT, sequenceIndex);
    }

    public long receivedMessageWithoutSequenceNumber(final int msgSeqNo, final int sequenceIndex)
    {
        return logout(msgSeqNo, NO_MSG_SEQ_NO, sequenceIndex);
    }

    public long rejectWhilstNotLoggedOn(final int msgSeqNo, final RejectReason reason, final int sequenceIndex)
    {
        return logout(msgSeqNo, NOT_LOGGED_ON_SESSION_REJECT_REASONS[reason.ordinal()], sequenceIndex);
    }

    public long heartbeat(final int msgSeqNo, final int sequenceIndex)
    {
        return heartbeat(null, 0, msgSeqNo, sequenceIndex);
    }

    public long heartbeat(
        final char[] testReqId,
        final int testReqIdLength,
        final int msgSeqNo,
        final int sequenceIndex)
    {
        final HeaderEncoder header = heartbeat.header();
        setupHeader(header, msgSeqNo);

        if (testReqId != null)
        {
            heartbeat.testReqID(testReqId, testReqIdLength);
        }
        else
        {
            heartbeat.resetTestReqID();
        }

        return send(heartbeat.encode(string, 0), HeartbeatDecoder.MESSAGE_TYPE, sequenceIndex, heartbeat);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final int refTagId,
        final byte[] refMsgType,
        final int refMsgTypeLength,
        final RejectReason reason,
        final int sequenceIndex)
    {
        reject.refTagID(refTagId);
        return reject(msgSeqNo, refSeqNum, refMsgType, refMsgTypeLength, reason, sequenceIndex);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final byte[] refMsgType,
        final int refMsgTypeLength,
        final RejectReason reason,
        final int sequenceIndex)
    {
        final int rejectReason = reason.representation();

        reject.refMsgType(refMsgType, refMsgTypeLength);
        reject.text(LOGGED_ON_SESSION_REJECT_REASONS[rejectReason]);

        return sendReject(msgSeqNo, refSeqNum, rejectReason, sequenceIndex);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason,
        final int sequenceIndex)
    {
        reject.refTagID(refTagId);
        return reject(msgSeqNo, refSeqNum, refMsgType, refMsgTypeLength, rejectReason, sequenceIndex);
    }

    public long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason,
        final int sequenceIndex)
    {
        reject.refMsgType(refMsgType, refMsgTypeLength);
        reject.text(LOGGED_ON_SESSION_REJECT_REASONS[rejectReason]);

        return sendReject(msgSeqNo, refSeqNum, rejectReason, sequenceIndex);
    }

    private long sendReject(final int msgSeqNo, final int refSeqNum, final int rejectReason, final int sequenceIndex)
    {
        final HeaderEncoder header = reject.header();
        setupHeader(header, msgSeqNo);

        reject.refSeqNum(refSeqNum);
        reject.sessionRejectReason(rejectReason);

        return send(reject.encode(string, 0), RejectDecoder.MESSAGE_TYPE, sequenceIndex, reject);
    }

    public long testRequest(final int msgSeqNo, final CharSequence testReqID, final int sequenceIndex)
    {
        final HeaderEncoder header = testRequest.header();
        setupHeader(header, msgSeqNo);

        testRequest.testReqID(testReqID);

        return send(testRequest.encode(string, 0), TestRequestDecoder.MESSAGE_TYPE, sequenceIndex, testRequest);
    }

    public long sequenceReset(final int msgSeqNo, final int newSeqNo, final int sequenceIndex)
    {
        final HeaderEncoder header = sequenceReset.header();
        setupHeader(header, msgSeqNo);

        sequenceReset.newSeqNo(newSeqNo);

        return send(sequenceReset.encode(string, 0), SequenceResetDecoder.MESSAGE_TYPE, sequenceIndex, sequenceReset);
    }

    private void setupHeader(final HeaderEncoder header, final int msgSeqNo)
    {
        final UtcTimestampEncoder timestampEncoder = this.timestampEncoder;
        header.sendingTime(timestampEncoder.buffer(), timestampEncoder.update(clock.time()));
        header.msgSeqNum(msgSeqNo);
    }

    private long send(final int length, final int messageType, final int sequenceIndex, final MessageEncoder encoder)
    {
        if (!libraryConnected)
        {
            return LIBRARY_DISCONNECTED;
        }

        final long position = gatewayPublication.saveMessage(
            buffer, 0, length, libraryId, messageType, sessionId, sequenceIndex, connectionId, OK);
        encoder.resetMessage();
        return position;
    }

    void libraryConnected(final boolean libraryConnected)
    {
        this.libraryConnected = libraryConnected;
    }
}
