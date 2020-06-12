/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.MessageTimingHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.protocol.GatewayPublication;

class EndPointFactory
{
    private final FixReceiverEndPoint.FixReceiverEndPointFormatters formatters =
        new FixReceiverEndPoint.FixReceiverEndPointFormatters();

    private final EngineConfiguration configuration;
    private final SessionContexts sessionContexts;
    private final GatewayPublication inboundLibraryPublication;
    private final FixCounters fixCounters;
    private final ErrorHandler errorHandler;
    private final GatewaySessions gatewaySessions;
    private final SenderSequenceNumbers senderSequenceNumbers;
    private final MessageTimingHandler messageTimingHandler;

    private SlowPeeker replaySlowPeeker;

    EndPointFactory(
        final EngineConfiguration configuration,
        final SessionContexts sessionContexts,
        final GatewayPublication inboundLibraryPublication,
        final FixCounters fixCounters,
        final ErrorHandler errorHandler,
        final GatewaySessions gatewaySessions,
        final SenderSequenceNumbers senderSequenceNumbers,
        final MessageTimingHandler messageTimingHandler)
    {
        this.configuration = configuration;
        this.sessionContexts = sessionContexts;
        this.inboundLibraryPublication = inboundLibraryPublication;
        this.fixCounters = fixCounters;
        this.errorHandler = errorHandler;
        this.gatewaySessions = gatewaySessions;
        this.senderSequenceNumbers = senderSequenceNumbers;
        this.messageTimingHandler = messageTimingHandler;
    }

    FixReceiverEndPoint receiverEndPoint(
        final TcpChannel channel,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final int libraryId,
        final Framer framer)
    {
        return new FixReceiverEndPoint(
            channel,
            configuration.receiverBufferSize(),
            inboundLibraryPublication,
            connectionId,
            sessionId,
            sequenceIndex,
            sessionContexts,
            fixCounters.messagesRead(connectionId, channel.remoteAddress()),
            framer,
            errorHandler,
            libraryId,
            gatewaySessions,
            configuration.clock(),
            framer.acceptorFixDictionaryLookup(),
            formatters);
    }

    SenderEndPoint senderEndPoint(
        final TcpChannel channel,
        final long connectionId,
        final int libraryId,
        final BlockablePosition libraryBlockablePosition,
        final Framer framer)
    {
        final String remoteAddress = channel.remoteAddress();
        final AtomicCounter bytesInBuffer = fixCounters.bytesInBuffer(connectionId, remoteAddress);
        return new SenderEndPoint(
            connectionId,
            libraryId,
            libraryBlockablePosition,
            replaySlowPeeker,
            channel,
            bytesInBuffer,
            fixCounters.invalidLibraryAttempts(connectionId, remoteAddress),
            errorHandler,
            framer,
            configuration.senderMaxBytesInBuffer(),
            configuration.slowConsumerTimeoutInMs(),
            System.currentTimeMillis(),
            senderSequenceNumbers.onNewSender(connectionId, bytesInBuffer),
            messageTimingHandler);
    }

    void replaySlowPeeker(final SlowPeeker replaySlowPeeker)
    {
        this.replaySlowPeeker = replaySlowPeeker;
    }
}
