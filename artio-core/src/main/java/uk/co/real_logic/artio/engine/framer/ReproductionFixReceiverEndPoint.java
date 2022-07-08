/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.ExclusivePublication;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.protocol.GatewayPublication;


// TODO: delete this class if possible?
public class ReproductionFixReceiverEndPoint extends FixReceiverEndPoint
{
    private final ExclusivePublication dataPublication;

    ReproductionFixReceiverEndPoint(
        final TcpChannel channel,
        final int bufferSize,
        final GatewayPublication publication,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final FixContexts fixContexts,
        final AtomicCounter messagesRead,
        final Framer framer,
        final ErrorHandler errorHandler,
        final int libraryId,
        final FixGatewaySessions gatewaySessions,
        final EpochNanoClock clock,
        final AcceptorFixDictionaryLookup acceptorFixDictionaryLookup,
        final FixReceiverEndPointFormatters formatters,
        final int throttleWindowInMs,
        final int throttleLimitOfMessages)
    {
        super(channel, bufferSize, publication, connectionId, sessionId, sequenceIndex, fixContexts, messagesRead,
            framer, errorHandler, libraryId, gatewaySessions, clock, acceptorFixDictionaryLookup, formatters,
            throttleWindowInMs, throttleLimitOfMessages);

        ((ReproductionTcpChannelSupplier.ReproductionTcpChannel)channel).receiverEndPoint(this);

        dataPublication = publication.dataPublication();

        System.out.println("ReproductionFixReceiverEndPoint.ReproductionFixReceiverEndPoint");
    }

    /*int poll()
    {
        if (isPaused || hasDisconnected())
        {
            return 0;
        }

        if (pendingAcceptorLogon != null)
        {
            return pollPendingLogon();
        }

        if (fullLength > 0)
        {
            final long position = dataPublication.offer(reproBuffer, 0, fullLength);
            if (position > 0)
            {
                System.out.println("ReproductionFixReceiverEndPoint.poll");

                messageDecoder.wrapAndApplyHeader(reproBuffer, 0, messageHeader);
                final long messageType = messageDecoder.messageType();
                gatewaySession.onMessage(reproBuffer, messageOffset, length, messageType, position);

                fullLength = 0;
            }

            return fullLength;
        }

        return 0;
    }*/

}
