/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.session;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.AsciiNumberFormatException;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.builder.Decoder;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.dictionary.SessionConstants;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.fields.UtcTimestampDecoder;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.messages.CancelOnDisconnectOption;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.builder.Validation.CODEC_VALIDATION_ENABLED;
import static uk.co.real_logic.artio.builder.Validation.isValidMsgType;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_LONG;
import static uk.co.real_logic.artio.messages.SessionState.AWAITING_LOGOUT;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.artio.session.Session.UNKNOWN;

public class SessionParser
{
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final UtcTimestampDecoder timestampDecoder;

    private AbstractLogonDecoder logon;
    private AbstractLogoutDecoder logout;
    private AbstractRejectDecoder reject;
    private AbstractTestRequestDecoder testRequest;
    private SessionHeaderDecoder header;
    private AbstractSequenceResetDecoder sequenceReset;
    private AbstractResendRequestDecoder resendRequest;
    private AbstractHeartbeatDecoder heartbeat;

    private final boolean validateCompIdsOnEveryMessage;
    private final OnMessageInfo messageInfo;
    private final SessionIdStrategy sessionIdStrategy;

    private final Session session;
    private final MessageValidationStrategy validationStrategy;
    private final ErrorHandler errorHandler;
    private CompositeKey compositeKey;

    public SessionParser(
        final Session session,
        final MessageValidationStrategy validationStrategy,
        final ErrorHandler errorHandler,
        final boolean validateCompIdsOnEveryMessage,
        final boolean validateTimeStrictly,
        final OnMessageInfo messageInfo,
        final SessionIdStrategy sessionIdStrategy)
    {
        this.session = session;
        this.validationStrategy = validationStrategy;
        this.errorHandler = errorHandler;
        this.validateCompIdsOnEveryMessage = validateCompIdsOnEveryMessage;
        this.messageInfo = messageInfo;
        this.sessionIdStrategy = sessionIdStrategy;
        this.timestampDecoder = new UtcTimestampDecoder(validateTimeStrictly);
    }

    public void fixDictionary(final FixDictionary fixDictionary)
    {
        logon = fixDictionary.makeLogonDecoder();
        logout = fixDictionary.makeLogoutDecoder();
        reject = fixDictionary.makeRejectDecoder();
        testRequest = fixDictionary.makeTestRequestDecoder();
        header = fixDictionary.makeHeaderDecoder();
        sequenceReset = fixDictionary.makeSequenceResetDecoder();
        heartbeat = fixDictionary.makeHeartbeatDecoder();
        resendRequest = fixDictionary.makeResendRequestDecoder();
    }

    public static String username(final AbstractLogonDecoder logon)
    {
        return logon.supportsUsername() ? logon.usernameAsString() : null;
    }

    public static String password(final AbstractLogonDecoder logon)
    {
        return logon.supportsPassword() ? logon.passwordAsString() : null;
    }

    public static CancelOnDisconnectOption cancelOnDisconnectType(
        final AbstractLogonDecoder logon,
        final CancelOnDisconnectOption defaultCancelOnDisconnectOption)
    {
        return logon.supportsCancelOnDisconnectType() && logon.hasCancelOnDisconnectType() ?
            CancelOnDisconnectOption.get(logon.cancelOnDisconnectType()) : defaultCancelOnDisconnectOption;
    }

    public static int cancelOnDisconnectTimeoutWindow(final AbstractLogonDecoder logon,
        final int defaultCancelOnDisconnectTimeoutWindow)
    {
        if (logon.supportsCODTimeoutWindow() && logon.hasCODTimeoutWindow())
        {
            return logon.cODTimeoutWindow();
        }
        else if (defaultCancelOnDisconnectTimeoutWindow != MISSING_INT)
        {
            return defaultCancelOnDisconnectTimeoutWindow;
        }
        else
        {
            return 0;
        }
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long messageType,
        final long position)
    {
        asciiBuffer.wrap(buffer);

        final Action action;

        try
        {
            if (messageType == LOGON_MESSAGE_TYPE)
            {
                action = onLogon(offset, length, position);
            }
            else if (messageType == LOGOUT_MESSAGE_TYPE)
            {
                action = onLogout(offset, length, position);
            }
            else if (messageType == HEARTBEAT_MESSAGE_TYPE)
            {
                action = onHeartbeat(offset, length, position);
            }
            else if (messageType == REJECT_MESSAGE_TYPE)
            {
                action = onReject(offset, length, position);
            }
            else if (messageType == TEST_REQUEST_MESSAGE_TYPE)
            {
                action = onTestRequest(offset, length, position);
            }
            else if (messageType == SEQUENCE_RESET_MESSAGE_TYPE)
            {
                action = onSequenceReset(offset, length, position);
            }
            else if (messageType == RESEND_REQUEST_MESSAGE_TYPE)
            {
                action = onResendRequest(offset, length, position);
            }
            else
            {
                action = onAnyOtherMessage(offset, length, position);
            }

            // Consider admin messages processed when they've been received by the session logic
            session.updateLastMessageProcessed();

            return action;
        }
        catch (final MalformedTagFormatException e)
        {
            return rejectAndHandleExceptionalMessage(e, messageType, e.refTagId(), position);
        }
        catch (final AsciiNumberFormatException e)
        {
            // We should just ignore the message when the first three fields are out of order.
            if (e.getMessage().contains("'^' is not a valid digit"))
            {
                return CONTINUE;
            }
            else
            {
                return rejectAndHandleExceptionalMessage(e, messageType, MISSING_INT, position);
            }
        }
        catch (final Exception e)
        {
            return rejectAndHandleExceptionalMessage(e, messageType, MISSING_INT, position);
        }
    }

    private Action rejectAndHandleExceptionalMessage(final Exception e, final long messageType, final int refTagId,
        final long position)
    {
        try
        {
            final Action action = rejectExceptionalMessage(messageType, refTagId, position);
            if (action == CONTINUE)
            {
                onError(e);
            }

            return action;
        }
        catch (final Exception secondEx)
        {
            // Case that sending the reject has also failed
            secondEx.addSuppressed(e);
            onError(secondEx);
            return CONTINUE;
        }
    }

    private Action rejectExceptionalMessage(final long messageType, final int refTagId, final long position)
    {
        if (messageType == LOGON_MESSAGE_TYPE)
        {
            final Action action = onExceptionalMessage(logon.header(), refTagId, position);
            if (action != ABORT)
            {
                session.logoutAndDisconnect();
            }
            return action;
        }
        else if (messageType == LOGOUT_MESSAGE_TYPE)
        {
            return onExceptionalMessage(logout.header(), refTagId, position);
        }
        else if (messageType == HEARTBEAT_MESSAGE_TYPE)
        {
            return onExceptionalMessage(heartbeat.header(), refTagId, position);
        }
        else if (messageType == REJECT_MESSAGE_TYPE)
        {
            return onExceptionalMessage(reject.header(), refTagId, position);
        }
        else if (messageType == TEST_REQUEST_MESSAGE_TYPE)
        {
            return onExceptionalMessage(testRequest.header(), refTagId, position);
        }
        else if (messageType == SEQUENCE_RESET_MESSAGE_TYPE)
        {
            return onExceptionalMessage(sequenceReset.header(), refTagId, position);
        }
        return onExceptionalMessage(header, refTagId, position);
    }

    private Action onExceptionalMessage(final SessionHeaderDecoder header, final int regTagId, final long position)
    {
        final int msgSeqNum = header.msgSeqNum();

        return session.onInvalidMessage(
            msgSeqNum,
            regTagId,
            header.msgType(),
            header.msgTypeLength(),
            SessionConstants.INCORRECT_DATA_FORMAT_FOR_VALUE,
            position);
    }

    private Action onHeartbeat(final int offset, final int length, final long position)
    {
        final AbstractHeartbeatDecoder heartbeat = this.heartbeat;

        heartbeat.reset();
        heartbeat.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = heartbeat.header();
        if (CODEC_VALIDATION_ENABLED && (!heartbeat.validate() || !validateHeader(header, position)))
        {
            return onCodecInvalidMessage(heartbeat, header, false, position);
        }
        else if (heartbeat.hasTestReqID())
        {
            final long origSendingTime = origSendingTimeInMs(header);
            final long sendingTime = sendingTimeInMs(header);
            final int testReqIDLength = heartbeat.testReqIDLength();
            final char[] testReqID = heartbeat.testReqID();
            final int msgSeqNum = header.msgSeqNum();
            final boolean possDup = isPossDup(header);

            return session.onHeartbeat(
                msgSeqNum,
                testReqID,
                testReqIDLength,
                sendingTime,
                origSendingTime,
                isPossDupOrResend(possDup, header),
                possDup,
                position);
        }
        else
        {
            return onMessage(header, position);
        }
    }

    private long sendingTimeInMs(final SessionHeaderDecoder header)
    {
        final byte[] sendingTime = header.sendingTime();
        return decodeTimestampInMs(sendingTime, header.sendingTimeLength(), SENDING_TIME);
    }

    private long decodeTimestampInMs(final byte[] sendingTime, final int length, final int refTagId)
    {
        try
        {
            return CODEC_VALIDATION_ENABLED ?
                timestampDecoder.decode(sendingTime, length) :
                MISSING_LONG;
        }
        catch (final Exception e)
        {
            throw new MalformedTagFormatException(refTagId, e);
        }
    }

    private Action onAnyOtherMessage(final int offset, final int length, final long position)
    {
        final SessionHeaderDecoder header = this.header;
        header.reset();
        header.decode(asciiBuffer, offset, length);

        final char[] msgType = header.msgType();
        final int msgTypeLength = header.msgTypeLength();
        if (CODEC_VALIDATION_ENABLED && (!isValidMsgType(msgType, msgTypeLength) || !validateHeader(header, position)))
        {
            final int msgSeqNum = header.msgSeqNum();
            if (!isDisconnectedOrAwaitingLogout())
            {
                return session.onInvalidMessageType(msgSeqNum, msgType, msgTypeLength, position);
            }
            else
            {
                messageInfo.isValid(false);
            }
        }
        else
        {
            return onMessage(header, position);
        }

        return CONTINUE;
    }

    private Action onMessage(final SessionHeaderDecoder header, final long position)
    {
        final long origSendingTime = origSendingTimeInMs(header);
        final long sendingTime = sendingTimeInMs(header);
        final boolean possDup = isPossDup(header);
        return session.onMessage(
            header.msgSeqNum(),
            header.msgType(),
            header.msgTypeLength(),
            sendingTime,
            origSendingTime,
            isPossDupOrResend(possDup, header),
            possDup,
            position);
    }

    private long origSendingTimeInMs(final SessionHeaderDecoder header)
    {
        if (header.hasOrigSendingTime())
        {
            return decodeTimestampInMs(header.origSendingTime(), header.origSendingTimeLength(), ORIG_SENDING_TIME);
        }
        return UNKNOWN;
    }

    private Action onResendRequest(final int offset, final int length, final long position)
    {
        final AbstractResendRequestDecoder resendRequest = this.resendRequest;

        resendRequest.reset();
        resendRequest.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = resendRequest.header();
        if (CODEC_VALIDATION_ENABLED && (!resendRequest.validate() || !validateHeader(header, position)))
        {
            return onCodecInvalidMessage(resendRequest, header, false, position);
        }
        else
        {
            final long origSendingTime = origSendingTimeInMs(header);
            final long sendingTime = sendingTimeInMs(header);
            final int beginSeqNo = resendRequest.beginSeqNo();
            final int endSeqNo = resendRequest.endSeqNo();
            final boolean possDup = isPossDup(header);
            return session.onResendRequest(
                header.msgSeqNum(),
                beginSeqNo,
                endSeqNo,
                isPossDupOrResend(possDup, header),
                possDup,
                sendingTime,
                origSendingTime,
                position,
                asciiBuffer,
                offset,
                length,
                resendRequest);
        }
    }

    private Action onSequenceReset(final int offset, final int length, final long position)
    {
        final AbstractSequenceResetDecoder sequenceReset = this.sequenceReset;

        sequenceReset.reset();
        sequenceReset.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = sequenceReset.header();
        if (CODEC_VALIDATION_ENABLED && (!sequenceReset.validate() || !validateHeader(header, position)))
        {
            return onCodecInvalidMessage(sequenceReset, header, false, position);
        }
        else
        {
            final boolean gapFillFlag = sequenceReset.hasGapFillFlag() && sequenceReset.gapFillFlag();
            final boolean possDup = isPossDup(header);
            return session.onSequenceReset(
                header.msgSeqNum(),
                sequenceReset.newSeqNo(),
                gapFillFlag,
                isPossDupOrResend(possDup, header),
                position);
        }
    }

    private Action onTestRequest(final int offset, final int length, final long position)
    {
        final AbstractTestRequestDecoder testRequest = this.testRequest;

        testRequest.reset();
        testRequest.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = testRequest.header();
        if (CODEC_VALIDATION_ENABLED && (!testRequest.validate() || !validateHeader(header, position)))
        {
            return onCodecInvalidMessage(testRequest, header, false, position);
        }
        else
        {
            final int msgSeqNo = header.msgSeqNum();
            final long origSendingTime = origSendingTimeInMs(header);
            final long sendingTimeInMs = sendingTimeInMs(header);
            final boolean possDup = isPossDup(header);
            return session.onTestRequest(
                msgSeqNo,
                testRequest.testReqID(),
                testRequest.testReqIDLength(),
                sendingTimeInMs,
                origSendingTime,
                isPossDupOrResend(possDup, header),
                possDup, position);
        }
    }

    private Action onReject(final int offset, final int length, final long position)
    {
        final AbstractRejectDecoder reject = this.reject;

        reject.reset();
        reject.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = reject.header();
        if (CODEC_VALIDATION_ENABLED && (!reject.validate() || !validateHeader(header, position)))
        {
            return onCodecInvalidMessage(reject, header, false, position);
        }
        else
        {
            final long origSendingTime = origSendingTimeInMs(header);
            final long sendingTime = sendingTimeInMs(header);
            final boolean possDup = isPossDup(header);
            return session.onReject(
                header.msgSeqNum(),
                sendingTime,
                origSendingTime,
                isPossDupOrResend(possDup, header),
                possDup,
                position);
        }
    }

    private Action onLogout(final int offset, final int length, final long position)
    {
        final AbstractLogoutDecoder logout = this.logout;

        logout.reset();
        logout.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = logout.header();
        if (CODEC_VALIDATION_ENABLED && (!logout.validate() || !validateHeader(header, position)))
        {
            return onCodecInvalidMessage(logout, header, false, position);
        }
        else
        {
            final long origSendingTimeInMs = origSendingTimeInMs(header);
            final long sendingTimeInMs = sendingTimeInMs(header);
            final boolean possDup = isPossDup(header);
            return session.onLogout(
                header.msgSeqNum(),
                sendingTimeInMs,
                origSendingTimeInMs,
                possDup,
                position);
        }
    }

    private Action onLogon(final int offset, final int length, final long position)
    {
        final AbstractLogonDecoder logon = this.logon;
        final Session session = this.session;

        logon.reset();
        logon.decode(asciiBuffer, offset, length);
        final SessionHeaderDecoder header = logon.header();
        final char[] beginString = header.beginString();
        final int beginStringLength = header.beginStringLength();
        if (CODEC_VALIDATION_ENABLED &&
            (!logon.validate() || !session.onBeginString(beginString, beginStringLength, true)))
        {
            return onCodecInvalidMessage(logon, header, true, position);
        }
        else
        {
            final long origSendingTime = origSendingTimeInMs(header);
            final String username = username(logon);
            final String password = password(logon);
            final CancelOnDisconnectOption cancelOnDisconnectOption = cancelOnDisconnectType(logon,
                session.cancelOnDisconnectOption());
            final int cancelOnDisconnectTimeoutWindowInMs = cancelOnDisconnectTimeoutWindow(logon,
                session.getCancelOnDisconnectTimeoutWindowInMs());
            final boolean possDup = isPossDup(header);

            return session.onLogon(
                logon.heartBtInt(),
                header.msgSeqNum(),
                sendingTimeInMs(header),
                origSendingTime,
                username,
                password,
                isPossDupOrResend(possDup, header),
                resetSeqNumFlag(logon),
                possDup,
                position,
                cancelOnDisconnectOption,
                cancelOnDisconnectTimeoutWindowInMs);
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
        errorHandler.onError(throwable);
    }

    private boolean resetSeqNumFlag(final AbstractLogonDecoder logon)
    {
        return logon.hasResetSeqNumFlag() && logon.resetSeqNumFlag();
    }

    private boolean validateHeader(final SessionHeaderDecoder header, final long position)
    {
        // Validate begin string
        if (!session.onBeginString(header.beginString(), header.beginStringLength(), false))
        {
            return false;
        }

        boolean validated = true;
        int rejectReason = RejectReason.OTHER.representation();

        int invalidTagId = validateCompIds(header);
        if (invalidTagId != 0)
        {
            validated = false;
            rejectReason = RejectReason.COMPID_PROBLEM.representation();
        }

        // Apply validation strategy
        if (validated)
        {
            try
            {
                validated = validationStrategy.validate(header);
                if (!validated)
                {
                    rejectReason = validationStrategy.rejectReason();
                    invalidTagId = validationStrategy.invalidTagId();
                }
            }
            catch (final Throwable throwable)
            {
                onStrategyError("validation", throwable, header.toString());
                validated = false;
            }
        }

        if (!validated)
        {
            session.onInvalidMessage(
                header.msgSeqNum(),
                invalidTagId,
                header.msgType(),
                header.msgTypeLength(),
                rejectReason,
                position);
            session.logoutRejectReason(rejectReason);
            session.startLogout();
            return false;
        }

        return true;
    }

    private int validateCompIds(final SessionHeaderDecoder header)
    {
        if (!validateCompIdsOnEveryMessage)
        {
            return 0;
        }

        // Case can happen when switching control of the Session between Library and Engine.
        return sessionIdStrategy.validateCompIds(compositeKey, header);
    }

    private Action onCodecInvalidMessage(
        final Decoder decoder,
        final SessionHeaderDecoder header,
        final boolean requestDisconnect,
        final long position)
    {
        if (!isDisconnectedOrAwaitingLogout())
        {
            final int msgTypeLength = header.msgTypeLength();

            if (header.msgSeqNum() == MISSING_INT)
            {
                final long origSendingTime = origSendingTimeInMs(header);
                final long sendingTime = sendingTimeInMs(header);
                final char[] msgType = header.msgType();
                return session.onMessage(MISSING_INT, msgType, msgTypeLength, sendingTime, origSendingTime, false,
                    false, position);
            }

            final Action action = session.onInvalidMessage(
                header.msgSeqNum(),
                decoder.invalidTagId(),
                header.msgType(),
                msgTypeLength,
                decoder.rejectReason(),
                position);

            if (action == CONTINUE && requestDisconnect)
            {
                return session.onInvalidFixDisconnect();
            }

            return action;
        }
        else
        {
            messageInfo.isValid(false);
        }

        if (requestDisconnect)
        {
            return session.onInvalidFixDisconnect();
        }

        return CONTINUE;
    }

    private boolean isDisconnectedOrAwaitingLogout()
    {
        final SessionState state = session.state();
        return state == DISCONNECTED || state == AWAITING_LOGOUT;
    }

    private boolean isPossDupOrResend(final boolean possDup, final SessionHeaderDecoder header)
    {
        return possDup || (header.hasPossResend() && header.possResend());
    }

    private boolean isPossDup(final SessionHeaderDecoder header)
    {
        return header.hasPossDupFlag() && header.possDupFlag();
    }

    public Session session()
    {
        return session;
    }

    public void sequenceIndex(final int sequenceIndex)
    {
        session.sequenceIndex(sequenceIndex);
    }

    public void sessionKey(final CompositeKey sessionKey)
    {
        this.compositeKey = sessionKey;
    }
}
