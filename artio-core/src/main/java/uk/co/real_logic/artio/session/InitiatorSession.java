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

import static uk.co.real_logic.artio.decoder.LogonDecoder.MESSAGE_TYPE_CHARS;

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
        final MutableAsciiBuffer asciiBuffer,
        final boolean enableLastMsgSeqNumProcessed)
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
            asciiBuffer,
            enableLastMsgSeqNumProcessed);
        this.resetSeqNum = resetSeqNum;
    }

    public Action onLogon(
        final int heartbeatInterval,
        final int msgSeqNum,
        final long sessionId,
        final CompositeKey sessionKey,
        final long sendingTime,
        final long origSendingTime,
        final String username,
        final String password,
        final boolean isPossDupOrResend,
        final boolean resetSeqNumFlag,
        final boolean possDup)
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
            return onResetSeqNumLogon(heartbeatInterval, username, password, logonTime, msgSeqNum);
        }

        final char[] msgType = MESSAGE_TYPE_CHARS;
        if (state() == SessionState.SENT_LOGON)
        {
            final int expectedSeqNo = expectedReceivedSeqNum();
            if (msgSeqNum == expectedSeqNo)
            {
                if (INITIAL_SEQUENCE_NUMBER == msgSeqNum)
                {
                    // Outgoing connections could be exchanging logons because of a network disconnection
                    // So we still only want this to occur on the initial logon.
                    logonTime(logonTime);
                }

                final long time = time();
                action = validateRequiredFieldsAndCodec(
                    msgSeqNum, time, msgType, msgType.length, sendingTime, origSendingTime, possDup);

                if (action != null)
                {
                    return action;
                }

                incNextReceivedInboundMessageTime(time);
                lastReceivedMsgSeqNum(msgSeqNum);
                setLogonState(heartbeatInterval, username, password);
                notifyLogonListener();
            }
            // Received the wrong sequence number from the acceptor
            else if (expectedSeqNo < msgSeqNum)
            {
                // NB: become active before the resend request because a user may want to send
                // Orders at this point.
                incNextReceivedInboundMessageTime(time());
                setLogonState(heartbeatInterval, username, password);
                notifyLogonListener();

                action = requestResend(expectedSeqNo, msgSeqNum);

                return action;
            }
            else /* expectedSeqNo > msgSeqNo */
            {
                // Disconnect with an error.

                return msgSeqNumTooLow(msgSeqNum, expectedSeqNo);
            }
        }
        else
        {
            // You've received a logon and you weren't expecting one and it hasn't got the resetSeqNumFlag set
            return onMessage(msgSeqNum, msgType, sendingTime, origSendingTime, isPossDupOrResend, possDup);
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
                sequenceIndex(),
                lastMsgSeqNumProcessed());
            if (position >= 0)
            {
                lastSentMsgSeqNum(sentSeqNum);
            }
            actions++;
        }

        return actions + super.poll(time);
    }
}
