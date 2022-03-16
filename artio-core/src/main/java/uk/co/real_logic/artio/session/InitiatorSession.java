/*
 * Copyright 2015-2022 Real Logic Limited.
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
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.messages.ConnectionType;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.EpochFractionClock;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

public class InitiatorSession extends InternalSession
{
    private final boolean resetSeqNum;

    public InitiatorSession(
        final int heartbeatInterval,
        final long connectionId,
        final EpochClock epochClock,
        final EpochNanoClock clock,
        final SessionProxy proxy,
        final GatewayPublication inboundPublication,
        final GatewayPublication outboundPublication,
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
        final boolean enableLastMsgSeqNumProcessed,
        final SessionCustomisationStrategy customisationStrategy,
        final OnMessageInfo messageInfo,
        final EpochFractionClock epochFractionClock,
        final boolean backpressureMessagesDuringReplay,
        final ResendRequestController resendRequestController,
        final int forcedHeartbeatIntervalInS)
    {
        super(
            heartbeatInterval,
            connectionId,
            clock,
            state,
            proxy,
            inboundPublication,
            outboundPublication,
            sessionIdStrategy,
            sendingTimeWindow,
            receivedMsgSeqNo,
            sentMsgSeqNo,
            libraryId,
            initialSentSequenceNumber,
            sequenceIndex,
            reasonableTransmissionTimeInMs,
            asciiBuffer,
            enableLastMsgSeqNumProcessed,
            customisationStrategy,
            messageInfo,
            epochFractionClock,
            ConnectionType.INITIATOR,
            backpressureMessagesDuringReplay,
            resendRequestController,
            forcedHeartbeatIntervalInS);
        this.resetSeqNum = resetSeqNum;
    }

    protected SessionState initialState()
    {
        return SessionState.SENT_LOGON;
    }

    protected Action respondToLogon(final int heartbeatInterval)
    {
        // Initiator sends its logon first, so has no need to reply
        return null;
    }

    public int poll(final long timeInNs)
    {
        int actions = 0;
        if (state() == SessionState.CONNECTED && id() != UNKNOWN)
        {
            state(SessionState.SENT_LOGON);
            final int heartbeatIntervalInS = (int)(heartbeatIntervalInMs() / 1000);
            final int sentSeqNum = resetSeqNum ? 1 : newSentSeqNum();
            final long position = proxy.sendLogon(sentSeqNum, heartbeatIntervalInS,
                username(),
                password(),
                resetSeqNum,
                sequenceIndex(),
                lastMsgSeqNumProcessed(),
                cancelOnDisconnectOption,
                getCancelOnDisconnectTimeoutWindowInMs());
            if (position >= 0)
            {
                lastSentMsgSeqNum(sentSeqNum);
            }
            actions++;
        }

        return actions + super.poll(timeInNs);
    }
}
