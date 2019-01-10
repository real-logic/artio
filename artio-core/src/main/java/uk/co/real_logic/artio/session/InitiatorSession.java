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
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static uk.co.real_logic.artio.decoder.LogonDecoder.MESSAGE_TYPE_BYTES;

public class InitiatorSession extends InternalSession
{
    private final boolean resetSeqNum;

    public InitiatorSession(
        final int heartbeatInterval,
        final long connectionId,
        final EpochClock clock,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final long sendingTimeWindow,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo,
        final int libraryId,
        final int initialSentSequenceNumber,
        final int sequenceIndex,
        final SessionState state,
        final boolean resetSeqNum,
        final long reasonableTransmissionTimeInMs,
        final MutableAsciiBuffer asciiBuffer)
    {
        super(
            heartbeatInterval,
            connectionId,
            clock,
            state,
            proxy,
            publication,
            sessionIdStrategy,
            sendingTimeWindow,
            receivedMsgSeqNo,
            sentMsgSeqNo,
            libraryId,
            initialSentSequenceNumber,
            sequenceIndex,
            reasonableTransmissionTimeInMs,
            asciiBuffer);
        this.resetSeqNum = resetSeqNum;
    }

    public Action onLogon(
        final int heartbeatInterval,
        final int msgSeqNo,
        final long sessionId,
        final CompositeKey sessionKey,
        final long sendingTime,
        final long origSendingTime,
        final String username,
        final String password,
        final boolean isPossDupOrResend,
        final boolean resetSeqNumFlag, final boolean possDup)
    {
        // We aren't checking CODEC_VALIDATION_ENABLED here because these are required values in order to
        // have a stable FIX connection.
        Action action = validateOrRejectHeartbeat(heartbeatInterval);
        if (action != null)
        {
            return action;
        }

        action = validateOrRejectSendingTime(sendingTime);
        if (action != null)
        {
            return action;
        }

        final long logonTime = sendingTime(sendingTime, origSendingTime);

        if (resetSeqNumFlag)
        {
            // Either we sent out a resetSeqNum flag when we connected or this session is already connected and they
            // have sent one to us to run an end of day.
            setupSession(sessionId, sessionKey);

            return onResetSeqNumLogon(heartbeatInterval, username, password, logonTime);
        }

        if (state() == SessionState.SENT_LOGON)
        {
            final int expectedSeqNo = expectedReceivedSeqNum();
            if (msgSeqNo == expectedSeqNo)
            {
                setupSession(sessionId, sessionKey);
                setLogonState(heartbeatInterval, username, password);

                if (INITIAL_SEQUENCE_NUMBER == msgSeqNo)
                {
                    // Outgoing connections could be exchanging logons because of a network disconnection
                    // So we still only want this to occur on the initial logon.
                    logonTime(logonTime);
                }

                notifyLogonListener();
                action = onMessage(msgSeqNo, MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend,
                    possDup);

                if (action == ABORT)
                {
                    return ABORT;
                }
            }
            // Received the wrong sequence number from the acceptor
            else if (expectedSeqNo < msgSeqNo)
            {
                // Setup the session and request a resend

                setupSession(sessionId, sessionKey);
                setLogonState(heartbeatInterval, username, password);
                notifyLogonListener();

                return requestResend(expectedSeqNo, msgSeqNo);
            }
            else /* expectedSeqNo > msgSeqNo */
            {
                // Disconnect with an error.

                return msgSeqNumTooLow(msgSeqNo, expectedSeqNo);
            }
        }
        else
        {
            // TODO: This is an error case, what is the right behaviour?
            // You've received a logon and you weren't expecting one and it hasn't got the resetSeqNumFlag set
            return onMessage(msgSeqNo, MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend, possDup);
        }

        return Action.CONTINUE;
    }

    public int poll(final long time)
    {
        int actions = 0;
        if (state() == SessionState.CONNECTED && id() != UNKNOWN)
        {
            state(SessionState.SENT_LOGON);
            final int heartbeatIntervalInS = (int)(heartbeatIntervalInMs() / 1000);
            final int sentSeqNum = resetSeqNum ? 1 : newSentSeqNum();
            final long position = proxy.logon(heartbeatIntervalInS,
                sentSeqNum,
                username(),
                password(),
                resetSeqNum,
                sequenceIndex());
            if (position >= 0)
            {
                lastSentMsgSeqNum(sentSeqNum);
            }
            actions++;
        }

        return actions + super.poll(time);
    }
}
