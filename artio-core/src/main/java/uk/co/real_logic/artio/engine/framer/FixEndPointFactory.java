/*
 * Copyright 2015-2022 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

class FixEndPointFactory
{
    private final FixReceiverEndPoint.FixReceiverEndPointFormatters formatters =
        new FixReceiverEndPoint.FixReceiverEndPointFormatters();

    private final EngineConfiguration configuration;
    private final FixContexts fixContexts;
    private final GatewayPublication inboundLibraryPublication;
    private final FixCounters fixCounters;
    private final ErrorHandler errorHandler;
    private final FixGatewaySessions gatewaySessions;
    private final SenderSequenceNumbers senderSequenceNumbers;
    private final MessageTimingHandler messageTimingHandler;

    private SlowPeeker replaySlowPeeker;

    FixEndPointFactory(
        final EngineConfiguration configuration,
        final FixContexts fixContexts,
        final GatewayPublication inboundLibraryPublication,
        final FixCounters fixCounters,
        final ErrorHandler errorHandler,
        final FixGatewaySessions gatewaySessions,
        final SenderSequenceNumbers senderSequenceNumbers,
        final MessageTimingHandler messageTimingHandler)
    {
        this.configuration = configuration;
        this.fixContexts = fixContexts;
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
            fixContexts,
            fixCounters.messagesRead(connectionId, channel.remoteAddr()),
            framer,
            errorHandler,
            libraryId,
            gatewaySessions,
            configuration.epochNanoClock(),
            framer.acceptorFixDictionaryLookup(),
            formatters,
            configuration.throttleWindowInMs(),
            configuration.throttleLimitOfMessages());
    }

    FixSenderEndPoint senderEndPoint(
        final TcpChannel channel,
        final long connectionId,
        final int libraryId,
        final BlockablePosition libraryBlockablePosition,
        final Framer framer)
    {
        final String remoteAddress = channel.remoteAddr();
        final AtomicCounter bytesInBuffer = fixCounters.bytesInBuffer(connectionId, remoteAddress);
        return new FixSenderEndPoint(
            connectionId,
            libraryId,
            libraryBlockablePosition,
            inboundLibraryPublication.dataPublication(),
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
            messageTimingHandler,
            configuration.maxConcurrentSessionReplays());
    }

    void replaySlowPeeker(final SlowPeeker replaySlowPeeker)
    {
        this.replaySlowPeeker = replaySlowPeeker;
    }
}
