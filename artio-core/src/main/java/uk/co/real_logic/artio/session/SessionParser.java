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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.builder.Decoder;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.fields.UtcTimestampDecoder;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.util.stream.Stream;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.builder.Validation.CODEC_VALIDATION_ENABLED;
import static uk.co.real_logic.artio.builder.Validation.isValidMsgType;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_LONG;
import static uk.co.real_logic.artio.messages.DisconnectReason.INVALID_FIX_MESSAGE;
import static uk.co.real_logic.artio.messages.SessionState.AWAITING_LOGOUT;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.artio.session.Session.UNKNOWN;

public class SessionParser
{
    private static final boolean HAS_USER_NAME_AND_PASSWORD = detectUsernameAndPassword();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final UtcTimestampDecoder timestampDecoder = new UtcTimestampDecoder();
    private final LogonDecoder logon = new LogonDecoder();
    private final LogoutDecoder logout = new LogoutDecoder();
    private final RejectDecoder reject = new RejectDecoder();
    private final TestRequestDecoder testRequest = new TestRequestDecoder();
    private final HeaderDecoder header = new HeaderDecoder();
    private final SequenceResetDecoder sequenceReset = new SequenceResetDecoder();
    private final HeartbeatDecoder heartbeat = new HeartbeatDecoder();

    // TODO: optimisation candidate to just move to copying bytes.
    private byte[] msgTypeBuffer = new byte[2];

    private final Session session;
    private final SessionIdStrategy sessionIdStrategy;
    private final MessageValidationStrategy validationStrategy;
    private ErrorHandler errorHandler;

    public SessionParser(
        final Session session,
        final SessionIdStrategy sessionIdStrategy,
        final MessageValidationStrategy validationStrategy,
        final ErrorHandler errorHandler) // nullable
    {
        this.session = session;
        this.sessionIdStrategy = sessionIdStrategy;
        this.validationStrategy = validationStrategy;
        this.errorHandler = errorHandler;
    }

    public static String username(final LogonDecoder logon)
    {
        return HAS_USER_NAME_AND_PASSWORD ? logon.usernameAsString() : null;
    }

    public static String password(final LogonDecoder logon)
    {
        return HAS_USER_NAME_AND_PASSWORD ? logon.passwordAsString() : null;
    }

    private static boolean detectUsernameAndPassword()
    {
        return Stream
            .of(LogonDecoder.class.getMethods())
            .anyMatch((method) -> "usernameAsString".equals(method.getName()));
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int messageType,
        final long sessionId)
    {
        asciiBuffer.wrap(buffer);

        switch (messageType)
        {
            case LogonDecoder.MESSAGE_TYPE:
                return onLogon(offset, length, sessionId);

            case LogoutDecoder.MESSAGE_TYPE:
                return onLogout(offset, length);

            case HeartbeatDecoder.MESSAGE_TYPE:
                return onHeartbeat(offset, length);

            case RejectDecoder.MESSAGE_TYPE:
                return onReject(offset, length);

            case TestRequestDecoder.MESSAGE_TYPE:
                return onTestRequest(offset, length);

            case SequenceResetDecoder.MESSAGE_TYPE:
                return onSequenceReset(offset, length);

            default:
                return onAnyOtherMessage(offset, length);
        }
    }

    private Action onHeartbeat(final int offset, final int length)
    {
        final HeartbeatDecoder heartbeat = this.heartbeat;

        heartbeat.reset();
        heartbeat.decode(asciiBuffer, offset, length);
        final HeaderDecoder header = heartbeat.header();
        if (CODEC_VALIDATION_ENABLED && (!heartbeat.validate() || !validateHeader(header)))
        {
            return onCodecInvalidMessage(heartbeat, header, false);
        }
        else if (heartbeat.hasTestReqID())
        {
            final long origSendingTime = origSendingTime(header);
            final long sendingTime = sendingTime(header);
            final int testReqIDLength = heartbeat.testReqIDLength();
            final char[] testReqID = heartbeat.testReqID();
            final int msgSeqNum = header.msgSeqNum();
            return session.onHeartbeat(
                msgSeqNum, testReqID, testReqIDLength, sendingTime, origSendingTime, isPossDup(header));
        }
        else
        {
            return onMessage(header);
        }
    }

    private long sendingTime(final HeaderDecoder header)
    {
        final byte[] sendingTime = header.sendingTime();
        return decodeTimestamp(sendingTime);
    }

    private long decodeTimestamp(final byte[] sendingTime)
    {
        return CODEC_VALIDATION_ENABLED ?
            timestampDecoder.decode(sendingTime, sendingTime.length) :
            MISSING_LONG;
    }

    private Action onAnyOtherMessage(final int offset, final int length)
    {
        final HeaderDecoder header = this.header;
        header.reset();
        header.decode(asciiBuffer, offset, length);

        final char[] msgType = header.msgType();
        final int msgTypeLength = header.msgTypeLength();
        if (CODEC_VALIDATION_ENABLED && (!isValidMsgType(msgType, msgTypeLength) || !validateHeader(header)))
        {
            final int msgSeqNum = header.msgSeqNum();
            if (!isDisconnectedOrAwaitingLogout())
            {
                return session.onInvalidMessageType(msgSeqNum, msgType, msgTypeLength);
            }
        }
        else
        {
            return onMessage(header);
        }

        return CONTINUE;
    }

    private Action onMessage(final HeaderDecoder header)
    {
        final long origSendingTime = origSendingTime(header);
        final long sendingTime = sendingTime(header);
        final int msgTypeLength = header.msgTypeLength();
        final byte[] msgType = extractMsgType(header, msgTypeLength);
        return session.onMessage(
            header.msgSeqNum(), msgType, msgTypeLength, sendingTime, origSendingTime, isPossDup(header));
    }

    private byte[] extractMsgType(final HeaderDecoder header, final int length)
    {
        msgTypeBuffer = CodecUtil.toBytes(header.msgType(), msgTypeBuffer, length);
        return msgTypeBuffer;
    }

    private long origSendingTime(final HeaderDecoder header)
    {
        return header.hasOrigSendingTime() ? decodeTimestamp(header.origSendingTime()) : UNKNOWN;
    }

    private Action onSequenceReset(final int offset, final int length)
    {
        final SequenceResetDecoder sequenceReset = this.sequenceReset;

        sequenceReset.reset();
        sequenceReset.decode(asciiBuffer, offset, length);
        final HeaderDecoder header = sequenceReset.header();
        if (CODEC_VALIDATION_ENABLED && (!sequenceReset.validate() || !validateHeader(header)))
        {
            return onCodecInvalidMessage(sequenceReset, header, false);
        }
        else
        {
            final boolean gapFillFlag = sequenceReset.hasGapFillFlag() && sequenceReset.gapFillFlag();
            return session.onSequenceReset(
                header.msgSeqNum(),
                sequenceReset.newSeqNo(),
                gapFillFlag,
                isPossDup(header));
        }
    }

    private Action onTestRequest(final int offset, final int length)
    {
        final TestRequestDecoder testRequest = this.testRequest;

        testRequest.reset();
        testRequest.decode(asciiBuffer, offset, length);
        final HeaderDecoder header = testRequest.header();
        if (CODEC_VALIDATION_ENABLED && (!testRequest.validate() || !validateHeader(header)))
        {
            return onCodecInvalidMessage(testRequest, header, false);
        }
        else
        {
            final int msgSeqNo = header.msgSeqNum();
            final long origSendingTime = origSendingTime(header);
            final long sendingTime = sendingTime(header);
            return session.onTestRequest(
                msgSeqNo,
                testRequest.testReqID(),
                testRequest.testReqIDLength(),
                sendingTime,
                origSendingTime,
                isPossDup(header));
        }
    }

    private Action onReject(final int offset, final int length)
    {
        final RejectDecoder reject = this.reject;

        reject.reset();
        reject.decode(asciiBuffer, offset, length);
        final HeaderDecoder header = reject.header();
        if (CODEC_VALIDATION_ENABLED && (!reject.validate() || !validateHeader(header)))
        {
            return onCodecInvalidMessage(reject, header, false);
        }
        else
        {
            final long origSendingTime = origSendingTime(header);
            final long sendingTime = sendingTime(header);
            return session.onReject(header.msgSeqNum(), sendingTime, origSendingTime, isPossDup(header));
        }
    }

    private Action onLogout(final int offset, final int length)
    {
        final LogoutDecoder logout = this.logout;

        logout.reset();
        logout.decode(asciiBuffer, offset, length);
        final HeaderDecoder header = logout.header();
        if (CODEC_VALIDATION_ENABLED && (!logout.validate() || !validateHeader(header)))
        {
            return onCodecInvalidMessage(logout, header, false);
        }
        else
        {
            final long origSendingTime = origSendingTime(header);
            final long sendingTime = sendingTime(header);
            return session.onLogout(header.msgSeqNum(), sendingTime, origSendingTime, isPossDup(header));
        }
    }

    private Action onLogon(final int offset, final int length, final long sessionId)
    {
        final LogonDecoder logon = this.logon;
        final Session session = this.session;

        logon.reset();
        logon.decode(asciiBuffer, offset, length);
        final HeaderDecoder header = logon.header();
        final char[] beginString = header.beginString();
        final int beginStringLength = header.beginStringLength();
        if (CODEC_VALIDATION_ENABLED &&
            (!logon.validate() || !session.onBeginString(beginString, beginStringLength, true)))
        {
            return onCodecInvalidMessage(logon, header, true);
        }
        else
        {
            final CompositeKey sessionKey = sessionIdStrategy.onAcceptLogon(header);
            final long origSendingTime = origSendingTime(header);
            final String username = username(logon);
            final String password = password(logon);

            return session.onLogon(
                logon.heartBtInt(),
                header.msgSeqNum(),
                sessionId,
                sessionKey,
                sendingTime(header),
                origSendingTime,
                username,
                password,
                isPossDup(header),
                resetSeqNumFlag(logon));
        }
    }

    private void onStrategyError(final String strategyName, final Throwable throwable, final String fixMessage)
    {
        final String message = String.format(
            "Exception thrown by %s strategy for connectionId=%d, [%s], defaulted to false",
            strategyName,
            session.connectionId(),
            fixMessage);

        onError(new FixGatewayException(message, throwable));
    }

    private void onError(final Throwable throwable)
    {
        // Library code should throw the exception to make users aware of it
        // Engine code should log it through the normal error handling process.
        if (errorHandler == null)
        {
            LangUtil.rethrowUnchecked(throwable);
        }
        else
        {
            errorHandler.onError(throwable);
        }
    }

    private boolean resetSeqNumFlag(final LogonDecoder logon)
    {
        return logon.hasResetSeqNumFlag() && logon.resetSeqNumFlag();
    }

    private boolean validateHeader(final HeaderDecoder header)
    {
        if (!session.onBeginString(header.beginString(), header.beginStringLength(), false))
        {
            return false;
        }

        boolean validated;
        try
        {
            validated = validationStrategy.validate(header);
        }
        catch (final Throwable throwable)
        {
            onStrategyError("validation", throwable, header.toString());
            validated = false;
        }

        if (!validated)
        {
            session.onInvalidMessage(
                header.msgSeqNum(),
                validationStrategy.invalidTagId(),
                header.msgType(),
                header.msgTypeLength(),
                validationStrategy.rejectReason());
            session.logoutRejectReason(validationStrategy.rejectReason());
            session.startLogout();
            return false;
        }

        return true;
    }

    private Action onCodecInvalidMessage(
        final Decoder decoder,
        final HeaderDecoder header,
        final boolean requestDisconnect)
    {
        if (!isDisconnectedOrAwaitingLogout())
        {
            final int msgTypeLength = header.msgTypeLength();

            if (header.msgSeqNum() == MISSING_INT)
            {
                final long origSendingTime = origSendingTime(header);
                final long sendingTime = sendingTime(header);
                final byte[] msgType = extractMsgType(header, msgTypeLength);
                return session.onMessage(MISSING_INT, msgType, msgTypeLength, sendingTime, origSendingTime, false);
            }

            final Action action = session.onInvalidMessage(
                header.msgSeqNum(),
                decoder.invalidTagId(),
                header.msgType(),
                msgTypeLength,
                decoder.rejectReason());

            if (action == CONTINUE && requestDisconnect)
            {
                return session.onRequestDisconnect(INVALID_FIX_MESSAGE);
            }

            return action;
        }

        if (requestDisconnect)
        {
            return session.onRequestDisconnect(INVALID_FIX_MESSAGE);
        }

        return CONTINUE;
    }

    private boolean isDisconnectedOrAwaitingLogout()
    {
        final SessionState state = session.state();
        return state == DISCONNECTED || state == AWAITING_LOGOUT;
    }

    private boolean isPossDup(final HeaderDecoder header)
    {
        return (header.hasPossDupFlag() && header.possDupFlag()) || (header.hasPossResend() && header.possResend());
    }

    public Session session()
    {
        return session;
    }

    public void sequenceIndex(final int sequenceIndex)
    {
        session.sequenceIndex(sequenceIndex);
    }
}
