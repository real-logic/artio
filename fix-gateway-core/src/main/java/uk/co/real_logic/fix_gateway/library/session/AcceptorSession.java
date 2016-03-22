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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;

public final class AcceptorSession extends Session
{

    public AcceptorSession(
        final int defaultInterval,
        final long connectionId,
        final EpochClock clock,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final char[] beginString,
        final long sendingTimeWindow,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo,
        final int libraryId,
        final int sessionBufferSize,
        final int initialSequenceNumber)
    {
        super(
            defaultInterval,
            connectionId,
            clock,
            SessionState.CONNECTED,
            proxy,
            publication,
            sessionIdStrategy,
            beginString,
            sendingTimeWindow,
            receivedMsgSeqNo,
            sentMsgSeqNo,
            libraryId,
            sessionBufferSize,
            initialSequenceNumber);
    }

    @Override
    public void onLogon(
        final int heartbeatInterval,
        final int msgSeqNo,
        final long sessionId,
        final CompositeKey sessionKey,
        final long sendingTime,
        final long origSendingTime,
        final String username,
        final String password,
        final boolean isPossDupOrResend)
    {
        id(sessionId);
        this.sessionKey = sessionKey;
        proxy.setupSession(sessionId, sessionKey);

        if (state() == SessionState.CONNECTED)
        {
            if (!validateHeartbeat(heartbeatInterval) || !validateSendingTime(sendingTime))
            {
                return;
            }

            final int expectedSeqNo = expectedReceivedSeqNum();
            if (expectedSeqNo == msgSeqNo)
            {
                heartbeatIntervalInS(heartbeatInterval);
                state(SessionState.ACTIVE);
                username(username);
                password(password);
                replyToLogon(heartbeatInterval);
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                state(SessionState.AWAITING_RESEND);
                replyToLogon(heartbeatInterval);
            }
            publication.saveLogon(libraryId, connectionId, sessionId);
        }
        onMessage(msgSeqNo, LogonDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    private void replyToLogon(int heartbeatInterval)
    {
        proxy.logon(heartbeatInterval, newSentSeqNum(), null, null);
    }

}
