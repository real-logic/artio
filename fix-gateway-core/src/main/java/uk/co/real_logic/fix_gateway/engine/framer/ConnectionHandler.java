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
package uk.co.real_logic.fix_gateway.engine.framer;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Handles incoming connections including setting up framers.
 *
 * Threadsafe.
 */
public class ConnectionHandler
{
    private final EngineConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final Streams inboundStreams;
    private final IdleStrategy idleStrategy;
    private final FixCounters fixCounters;
    private final ErrorHandler errorHandler;

    public ConnectionHandler(
        final EngineConfiguration configuration,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final Streams inboundStreams,
        final IdleStrategy idleStrategy,
        final FixCounters fixCounters,
        final ErrorHandler errorHandler)
    {
        this.configuration = configuration;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.inboundStreams = inboundStreams;
        this.idleStrategy = idleStrategy;
        this.fixCounters = fixCounters;
        this.errorHandler = errorHandler;
    }

    public ReceiverEndPoint receiverEndPoint(
        final SocketChannel channel,
        final long connectionId,
        final long sessionId,
        final int libraryId,
        final Framer framer,
        final ReliefValve reliefValve,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final boolean resetSequenceNumbers) throws IOException
    {
        return new ReceiverEndPoint(
            channel,
            configuration.receiverBufferSize(),
            inboundPublication(reliefValve),
            connectionId,
            sessionId,
            sessionIdStrategy,
            sessionIds,
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex,
            fixCounters.messagesRead(connectionId, channel.getRemoteAddress()),
            framer,
            errorHandler,
            libraryId,
            resetSequenceNumbers
        );
    }

    public GatewayPublication inboundPublication(final ReliefValve reliefValve)
    {
        return inboundStreams.gatewayPublication(idleStrategy, reliefValve);
    }

    public SenderEndPoint senderEndPoint(
        final SocketChannel channel,
        final long connectionId,
        final int libraryId,
        final Framer framer,
        final ReliefValve reliefValue) throws IOException
    {
        return new SenderEndPoint(
            connectionId,
            libraryId,
            channel,
            idleStrategy,
            fixCounters.messagesWritten(connectionId, channel.getRemoteAddress()),
            errorHandler,
            framer,
            reliefValue,
            configuration.senderMaxAttempts());
    }

}
