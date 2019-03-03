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
package uk.co.real_logic.artio.session;

import org.agrona.concurrent.EpochClock;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.AsciiFormatter;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.List;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.fields.RejectReason.VALUE_IS_INCORRECT;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;
import static uk.co.real_logic.artio.session.Session.LIBRARY_DISCONNECTED;

/**
 * Encapsulates sending messages relating to sessions
 */
public class SessionProxy
{
    static final int NO_LAST_MSG_SEQ_NUM_PROCESSED = -1;

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
                "Invalid Logon message: SendingTime accuracy problem, field=52, reason=%s",
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
    private final MutableAsciiBuffer buffer;
    private final GatewayPublication gatewayPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final EpochClock clock;
    private final long connectionId;
    private final int libraryId;
    private long sessionId;
    private boolean libraryConnected = true;
    private boolean seqNumResetRequested = false;

    public SessionProxy(
        final MutableAsciiBuffer buffer,
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
        this.buffer = buffer;
        lowSequenceNumber = new AsciiFormatter("MsgSeqNum too low, expecting %s but received %s");
        timestampEncoder.initialise(clock.time());
    }

    void setupSession(final long sessionId, final CompositeKey sessionKey)
    {
        requireNonNull(sessionKey, "sessionKey");

        this.sessionId = sessionId;
        for (final HeaderEncoder header : headers)
        {
            sessionIdStrategy.setupSession(sessionKey, header);
        }
    }

    long resendRequest(
        final int msgSeqNo,
        final int beginSeqNo,
        final int endSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        final HeaderEncoder header = resendRequest.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
        final long result = resendRequest.encode(buffer, 0);
        return send(result, ResendRequestDecoder.MESSAGE_TYPE, sequenceIndex, resendRequest, msgSeqNo);
    }

    long requestDisconnect(final long connectionId, final DisconnectReason reason)
    {
        return gatewayPublication.saveRequestDisconnect(libraryId, connectionId, reason);
    }

    long logon(
        final int heartbeatIntervalInS,
        final int msgSeqNo,
        final String username,
        final String password,
        final boolean resetSeqNumFlag,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        final HeaderEncoder header = logon.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);

        logon
            .heartBtInt(heartbeatIntervalInS)
            .resetSeqNumFlag(resetSeqNumFlag)
            .encryptMethod(0); // may have been previously reset

        if (notNullOrEmpty(username))
        {
            logon.username(username);
        }
        if (notNullOrEmpty(password))
        {
            logon.password(password);
        }
        customisationStrategy.configureLogon(logon, sessionId);
        seqNumResetRequested = logon.resetSeqNumFlag(); // get customized or default

        final long result = logon.encode(buffer, 0);
        return send(result, LogonDecoder.MESSAGE_TYPE, sequenceIndex, logon, msgSeqNo);
    }

    private boolean notNullOrEmpty(final String string)
    {
        return string != null && string.length() > 0;
    }

    long logout(final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return logout(msgSeqNo, null, 0, sequenceIndex, lastMsgSeqNumProcessed);
    }

    long logout(
        final int msgSeqNo, final int sequenceIndex, final int rejectReason, final int lastMsgSeqNumProcessed)
    {
        final byte[] reasonText = LOGGED_ON_SESSION_REJECT_REASONS[rejectReason];
        return logout(msgSeqNo, reasonText, reasonText.length, sequenceIndex, lastMsgSeqNumProcessed);
    }

    private long logout(
        final int msgSeqNo, final byte[] text, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return logout(msgSeqNo, text, text.length, sequenceIndex, lastMsgSeqNumProcessed);
    }

    private long logout(
        final int msgSeqNo,
        final byte[] text,
        final int length,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        final HeaderEncoder header = logout.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);

        if (text != null)
        {
            logout.text(text, length);
        }

        customisationStrategy.configureLogout(logout, sessionId);
        final long result = logout.encode(buffer, 0);
        return send(result, LogoutDecoder.MESSAGE_TYPE, sequenceIndex, logout, msgSeqNo);
    }

    long lowSequenceNumberLogout(
        final int msgSeqNo,
        final int expectedSeqNo,
        final int receivedSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        lowSequenceNumber
            .with(expectedSeqNo)
            .with(receivedSeqNo);

        final long position = logout(
            msgSeqNo, lowSequenceNumber.value(), lowSequenceNumber.length(), sequenceIndex, lastMsgSeqNumProcessed);
        lowSequenceNumber.clear();

        return position;
    }

    long incorrectBeginStringLogout(
        final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return logout(msgSeqNo, INCORRECT_BEGIN_STRING, sequenceIndex, lastMsgSeqNumProcessed);
    }

    long negativeHeartbeatLogout(
        final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return logout(msgSeqNo, NEGATIVE_HEARTBEAT, sequenceIndex, lastMsgSeqNumProcessed);
    }

    long receivedMessageWithoutSequenceNumber(
        final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return logout(msgSeqNo, NO_MSG_SEQ_NO, sequenceIndex, lastMsgSeqNumProcessed);
    }

    long rejectWhilstNotLoggedOn(
        final int msgSeqNo, final RejectReason reason, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return logout(
            msgSeqNo, NOT_LOGGED_ON_SESSION_REJECT_REASONS[reason.ordinal()], sequenceIndex, lastMsgSeqNumProcessed);
    }

    public long heartbeat(final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return heartbeat(
            null, 0, msgSeqNo, sequenceIndex, lastMsgSeqNumProcessed);
    }

    public long heartbeat(
        final char[] testReqId,
        final int testReqIdLength,
        final int msgSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        final HeaderEncoder header = heartbeat.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);

        if (testReqId != null)
        {
            heartbeat.testReqID(testReqId, testReqIdLength);
        }
        else
        {
            heartbeat.resetTestReqID();
        }

        final long result = heartbeat.encode(buffer, 0);
        return send(result, HeartbeatDecoder.MESSAGE_TYPE, sequenceIndex, heartbeat, msgSeqNo);
    }

    long reject(
        final int msgSeqNo,
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        if (refTagId != MISSING_INT)
        {
            reject.refTagID(refTagId);
        }
        else
        {
            reject.resetRefTagID();
        }

        reject.refMsgType(refMsgType, refMsgTypeLength);
        reject.text(LOGGED_ON_SESSION_REJECT_REASONS[rejectReason]);

        final HeaderEncoder header = reject.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);

        reject.refSeqNum(refSeqNum);
        reject.sessionRejectReason(rejectReason);

        final long result = reject.encode(buffer, 0);
        return send(result, RejectDecoder.MESSAGE_TYPE, sequenceIndex, reject, msgSeqNo);
    }

    public long testRequest(
        final int msgSeqNo, final CharSequence testReqID, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        final HeaderEncoder header = testRequest.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);

        testRequest.testReqID(testReqID);

        final long result = testRequest.encode(buffer, 0);
        return send(result, TestRequestDecoder.MESSAGE_TYPE, sequenceIndex, testRequest, msgSeqNo);
    }

    public long sequenceReset(
        final int msgSeqNo, final int newSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        final HeaderEncoder header = sequenceReset.header();
        setupHeader(header, msgSeqNo, lastMsgSeqNumProcessed);

        sequenceReset.newSeqNo(newSeqNo);

        final long result = sequenceReset.encode(buffer, 0);
        return send(result, SequenceResetDecoder.MESSAGE_TYPE, sequenceIndex, sequenceReset, msgSeqNo);
    }

    private void setupHeader(final HeaderEncoder header, final int msgSeqNo, final int lastMsgSeqNumProcessed)
    {
        final UtcTimestampEncoder timestampEncoder = this.timestampEncoder;
        header.sendingTime(timestampEncoder.buffer(), timestampEncoder.update(clock.time()));
        header.msgSeqNum(msgSeqNo);

        if (lastMsgSeqNumProcessed != NO_LAST_MSG_SEQ_NUM_PROCESSED)
        {
            header.lastMsgSeqNumProcessed(lastMsgSeqNumProcessed);
        }
    }

    private long send(
        final long result,
        final int messageType,
        final int sequenceIndex,
        final Encoder encoder,
        final int msgSeqNo)
    {
        if (!libraryConnected)
        {
            return LIBRARY_DISCONNECTED;
        }

        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);
        final long position = gatewayPublication.saveMessage(
            buffer, offset, length,
            libraryId, messageType, sessionId, sequenceIndex, connectionId, OK, msgSeqNo);
        encoder.resetMessage();
        return position;
    }

    void libraryConnected(final boolean libraryConnected)
    {
        this.libraryConnected = libraryConnected;
    }

    boolean seqNumResetRequested()
    {
        return seqNumResetRequested;
    }
}
