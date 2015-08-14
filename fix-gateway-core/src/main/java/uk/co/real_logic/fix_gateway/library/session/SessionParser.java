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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.builder.Decoder;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static uk.co.real_logic.fix_gateway.builder.Validation.CODEC_VALIDATION_ENABLED;
import static uk.co.real_logic.fix_gateway.builder.Validation.isValidMsgType;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.library.session.Session.UNKNOWN;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.AWAITING_LOGOUT;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.DISCONNECTED;

public class SessionParser
{

    private final AsciiFlyweight string = new AsciiFlyweight();
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
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;

    public SessionParser(
        final Session session,
        final SessionIdStrategy sessionIdStrategy,
        final AuthenticationStrategy authenticationStrategy,
        final MessageValidationStrategy validationStrategy)
    {
        this.session = session;
        this.sessionIdStrategy = sessionIdStrategy;
        this.authenticationStrategy = authenticationStrategy;
        this.validationStrategy = validationStrategy;
    }

    public boolean onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int messageType,
        final long sessionId)
    {
        string.wrap(buffer);

        switch (messageType)
        {
            case LogonDecoder.MESSAGE_TYPE:
                onLogon(offset, length, sessionId);
                break;

            case LogoutDecoder.MESSAGE_TYPE:
            {
                onLogout(offset, length);
                break;
            }

            case HeartbeatDecoder.MESSAGE_TYPE:
            {
                onHeartbeat(offset, length);
                break;
            }

            case RejectDecoder.MESSAGE_TYPE:
            {
                onReject(offset, length);
                break;
            }

            case TestRequestDecoder.MESSAGE_TYPE:
            {
                onTestRequest(offset, length);
                break;
            }

            case SequenceResetDecoder.MESSAGE_TYPE:
            {
                onSequenceReset(offset, length);
                break;
            }

            default:
            {
                onAnyOtherMessage(offset, length);
                break;
            }
        }

        return session.isConnected();
    }

    private void onHeartbeat(final int offset, final int length)
    {
        heartbeat.reset();
        heartbeat.decode(string, offset, length);
        final HeaderDecoder header = heartbeat.header();
        if (CODEC_VALIDATION_ENABLED && (!heartbeat.validate() || !validateHeader(header)))
        {
            onCodecInvalidMessage(heartbeat, header);
        }
        else if (heartbeat.hasTestReqID())
        {
            final long origSendingTime = getSendingTime(header);
            final long sendingTime = header.sendingTime();
            final int testReqIDLength = heartbeat.testReqIDLength();
            final char[] testReqID = heartbeat.testReqID();
            final int msgSeqNum = header.msgSeqNum();
            session.onHeartbeat(
                msgSeqNum, testReqID, testReqIDLength, sendingTime, origSendingTime, isPossDup(header));
        }
        else
        {
            onMessage(header);
        }
    }

    private void onAnyOtherMessage(final int offset, final int length)
    {
        final HeaderDecoder header = this.header;
        header.reset();
        header.decode(string, offset, length);

        final char[] msgType = header.msgType();
        final int msgTypeLength = header.msgTypeLength();
        if (CODEC_VALIDATION_ENABLED && (!isValidMsgType(msgType, msgTypeLength) || !validateHeader(header)))
        {
            final int msgSeqNum = header.msgSeqNum();
            session.onInvalidMessageType(msgSeqNum, msgType, msgTypeLength);
        }
        else
        {
            onMessage(header);
        }
    }

    private void onMessage(final HeaderDecoder header)
    {
        final long origSendingTime = getSendingTime(header);
        final long sendingTime = header.sendingTime();
        final int msgTypeLength = header.msgTypeLength();
        final byte[] msgType = extractMsgType(header, msgTypeLength);
        session.onMessage(
            header.msgSeqNum(), msgType, msgTypeLength, sendingTime, origSendingTime, isPossDup(header));
    }

    private byte[] extractMsgType(final HeaderDecoder header, final int length)
    {
        msgTypeBuffer = CodecUtil.toBytes(header.msgType(), msgTypeBuffer, length);
        return msgTypeBuffer;
    }

    private long getSendingTime(final HeaderDecoder header)
    {
        return header.hasOrigSendingTime() ? header.origSendingTime() : UNKNOWN;
    }

    private void onSequenceReset(final int offset, final int length)
    {
        sequenceReset.reset();
        sequenceReset.decode(string, offset, length);
        final HeaderDecoder header = sequenceReset.header();
        if (CODEC_VALIDATION_ENABLED && (!sequenceReset.validate() || !validateHeader(header)))
        {
            onCodecInvalidMessage(sequenceReset, header);
        }
        else
        {
            final boolean gapFillFlag = sequenceReset.hasGapFillFlag() && sequenceReset.gapFillFlag();
            session.onSequenceReset(
                header.msgSeqNum(),
                sequenceReset.newSeqNo(),
                gapFillFlag,
                isPossDup(header));
        }
    }

    private void onTestRequest(final int offset, final int length)
    {
        testRequest.reset();
        testRequest.decode(string, offset, length);
        final HeaderDecoder header = testRequest.header();
        if (CODEC_VALIDATION_ENABLED && (!testRequest.validate() || !validateHeader(header)))
        {
            onCodecInvalidMessage(testRequest, header);
        }
        else
        {
            final int msgSeqNo = header.msgSeqNum();
            final long origSendingTime = getSendingTime(header);
            final long sendingTime = header.sendingTime();
            session.onTestRequest(
                msgSeqNo,
                testRequest.testReqID(),
                testRequest.testReqIDLength(),
                sendingTime,
                origSendingTime,
                isPossDup(header));
        }
    }

    private void onReject(final int offset, final int length)
    {
        reject.reset();
        reject.decode(string, offset, length);
        final HeaderDecoder header = reject.header();
        if (CODEC_VALIDATION_ENABLED && (!reject.validate() || !validateHeader(header)))
        {
            onCodecInvalidMessage(reject, header);
        }
        else
        {
            final long origSendingTime = getSendingTime(header);
            final long sendingTime = header.sendingTime();
            session.onReject(header.msgSeqNum(), sendingTime, origSendingTime, isPossDup(header));
        }
    }

    private void onLogout(final int offset, final int length)
    {
        logout.reset();
        logout.decode(string, offset, length);
        final HeaderDecoder header = logout.header();
        if (CODEC_VALIDATION_ENABLED && (!logout.validate() || !validateHeader(header)))
        {
            onCodecInvalidMessage(logout, header);
        }
        else
        {
            final long origSendingTime = getSendingTime(header);
            final long sendingTime = header.sendingTime();
            session.onLogout(header.msgSeqNum(), sendingTime, origSendingTime, isPossDup(header));
        }
    }

    private void onLogon(final int offset, final int length, final long sessionId)
    {
        logon.reset();
        logon.decode(string, offset, length);
        final HeaderDecoder header = logon.header();
        final char[] beginString = header.beginString();
        final int beginStringLength = header.beginStringLength();
        if (CODEC_VALIDATION_ENABLED && (!logon.validate() || !session.onBeginString(beginString, beginStringLength, true)))
        {
            if (!onCodecInvalidMessage(logon, header))
            {
                session.requestDisconnect();
            }
        }
        else
        {
            if (authenticationStrategy.authenticate(logon))
            {
                final Object sessionKey = sessionIdStrategy.onAcceptorLogon(header);

                final long origSendingTime = getSendingTime(header);
                session.onLogon(
                    logon.heartBtInt(),
                    header.msgSeqNum(),
                    sessionId,
                    sessionKey,
                    header.sendingTime(),
                    origSendingTime,
                    isPossDup(header));
            }
            else
            {
                session.requestDisconnect();
            }
        }
    }

    private boolean validateHeader(final HeaderDecoder header)
    {
        if (!session.onBeginString(header.beginString(), header.beginStringLength(), false))
        {
            return false;
        }

        if (!validationStrategy.validate(header))
        {
            session.onInvalidMessage(
                header.msgSeqNum(),
                validationStrategy.invalidTagId(),
                header.msgType(),
                header.msgTypeLength(),
                validationStrategy.rejectReason());
            return false;
        }

        return true;
    }

    private boolean onCodecInvalidMessage(final Decoder decoder, final HeaderDecoder header)
    {
        final SessionState state = session.state();
        if (state != DISCONNECTED && state != AWAITING_LOGOUT)
        {
            final int msgTypeLength = header.msgTypeLength();

            if (header.msgSeqNum() == MISSING_INT)
            {
                final long origSendingTime = getSendingTime(header);
                final long sendingTime = header.sendingTime();
                final byte[] msgType = extractMsgType(header, msgTypeLength);
                session.onMessage(MISSING_INT, msgType, msgTypeLength, sendingTime, origSendingTime, false);
                return true;
            }

            session.onInvalidMessage(
                header.msgSeqNum(),
                decoder.invalidTagId(),
                header.msgType(),
                msgTypeLength,
                decoder.rejectReason());

            return false;
        }

        return false;
    }

    private boolean isPossDup(final HeaderDecoder header)
    {
        return (header.hasPossDupFlag() && header.possDupFlag())
            || (header.hasPossResend() && header.possResend());
    }

    public Session session()
    {
        return session;
    }
}
