/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.protocol.GatewayPublication;

class EndPointFactory
{
    private final EngineConfiguration configuration;
    private final SessionContexts sessionContexts;
    private final GatewayPublication inboundLibraryPublication;
    private final FixCounters fixCounters;
    private final ErrorHandler errorHandler;
    private final GatewaySessions gatewaySessions;
    private final SenderSequenceNumbers senderSequenceNumbers;

    private SlowPeeker replaySlowPeeker;

    EndPointFactory(
        final EngineConfiguration configuration,
        final SessionContexts sessionContexts,
        final GatewayPublication inboundLibraryPublication,
        final FixCounters fixCounters,
        final ErrorHandler errorHandler,
        final GatewaySessions gatewaySessions,
        final SenderSequenceNumbers senderSequenceNumbers)
    {
        this.configuration = configuration;
        this.sessionContexts = sessionContexts;
        this.inboundLibraryPublication = inboundLibraryPublication;
        this.fixCounters = fixCounters;
        this.errorHandler = errorHandler;
        this.gatewaySessions = gatewaySessions;
        this.senderSequenceNumbers = senderSequenceNumbers;
    }

    ReceiverEndPoint receiverEndPoint(
        final TcpChannel channel,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final int libraryId,
        final Framer framer,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex)
    {
        return new ReceiverEndPoint(
            channel,
            configuration.receiverBufferSize(),
            inboundLibraryPublication,
            connectionId,
            sessionId,
            sequenceIndex,
            sessionContexts,
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex,
            fixCounters.messagesRead(connectionId, channel.remoteAddress()),
            framer,
            errorHandler,
            libraryId,
            gatewaySessions
        );
    }

    SenderEndPoint senderEndPoint(
        final TcpChannel channel,
        final long connectionId,
        final int libraryId,
        final BlockablePosition libraryBlockablePosition,
        final Framer framer)
    {
        final String remoteAddress = channel.remoteAddress();
        return new SenderEndPoint(
            connectionId,
            libraryId,
            libraryBlockablePosition,
            replaySlowPeeker,
            channel,
            fixCounters.bytesInBuffer(connectionId, remoteAddress),
            fixCounters.invalidLibraryAttempts(connectionId, remoteAddress),
            errorHandler,
            framer,
            configuration.senderMaxBytesInBuffer(),
            configuration.slowConsumerTimeoutInMs(),
            System.currentTimeMillis(),
            senderSequenceNumbers.onNewSender(connectionId));
    }

    void replaySlowPeeker(final SlowPeeker replaySlowPeeker)
    {
        this.replaySlowPeeker = replaySlowPeeker;
    }
}
