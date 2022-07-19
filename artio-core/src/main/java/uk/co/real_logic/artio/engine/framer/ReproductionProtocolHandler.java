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

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.engine.EngineReproductionClock;
import uk.co.real_logic.artio.engine.logger.ReproductionFixProtocolConsumer;
import uk.co.real_logic.artio.messages.ApplicationHeartbeatDecoder;
import uk.co.real_logic.artio.messages.ConnectDecoder;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;

public class ReproductionProtocolHandler implements ReproductionFixProtocolConsumer
{
    public static final boolean REPRO_DBG_ENABLED = DebugLogger.isEnabled(LogTag.REPRODUCTION);

    // Decode protocol for relevant messages and hand them off down the line to
    // Artio component that is responsible for dealing with the event in normal times.
    // components should block until operation complete to ensure ordering is maintained.
    // TODO: need a way of controlling the sending of application heartbeats as well

    private final ReproductionTcpChannelSupplier tcpChannelSupplier;
    private final EngineReproductionClock clock;
    private final ErrorHandler errorHandler;

    private long connectionId = NO_CONNECTION_ID;
    private Int2ObjectHashMap<LiveLibraryInfo> idToLibrary;

    // Enforce only a single operation is in progress
    private boolean operationInProgress = false;

    public ReproductionProtocolHandler(
        final ReproductionTcpChannelSupplier tcpChannelSupplier,
        final EngineReproductionClock clock,
        final ErrorHandler errorHandler)
    {
        this.tcpChannelSupplier = tcpChannelSupplier;
        this.clock = clock;
        this.errorHandler = errorHandler;

        tcpChannelSupplier.registerEndOperation(this::endOperation);
    }

    public void onMessage(
        final FixMessageDecoder message,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ArtioLogHeader header)
    {
        if (REPRO_DBG_ENABLED)
        {
            DebugLogger.log(LogTag.REPRODUCTION,
                "ReproductionProtocolHandler.onMessage: ", buffer, offset, length);
        }
        startOperation();
        clock.advanceTimeTo(message.timestamp());
        validateLibraryId(message.libraryId());

        final int initialOffset = message.initialOffset() - MessageHeaderDecoder.ENCODED_LENGTH;
        final int fullLength = (offset + length) - initialOffset;
        final int messageOffset = offset - initialOffset;
        final long connectionId = message.connection();
        tcpChannelSupplier.enqueueMessage(connectionId, buffer, initialOffset, fullLength, messageOffset, length);
    }

    private void validateLibraryId(final int libraryId)
    {
        if (libraryId != ENGINE_LIBRARY_ID && !idToLibrary.containsKey(libraryId))
        {
            errorHandler.onError(new IllegalStateException(
                "Unknown library Id: " + libraryId + " not in " + idToLibrary.keySet()));
        }
    }

    public void onConnect(
        final ConnectDecoder connectDecoder,
        final DirectBuffer buffer,
        final int start,
        final int length)
    {
        System.out.println("ReproductionProtocolHandler.onConnect");
        startOperation();
        clock.advanceTimeTo(connectDecoder.timestamp());
        connectionId = connectDecoder.connection();
        tcpChannelSupplier.enqueueConnect(connectDecoder);
    }

    public void onApplicationHeartbeat(
        final ApplicationHeartbeatDecoder decoder, final DirectBuffer buffer, final int start, final int length)
    {
        System.out.println("ReproductionProtocolHandler.onApplicationHeartbeat");
        clock.advanceTimeTo(decoder.timestampInNs());
        validateLibraryId(decoder.libraryId());
    }

    public long newConnectionId()
    {
        System.out.println("ReproductionProtocolHandler.newConnectionId");
        if (connectionId == NO_CONNECTION_ID)
        {
            final IllegalStateException ex = new IllegalStateException("Unknown connection id");
            errorHandler.onError(ex);
            throw ex;
        }

        final long connectionId = this.connectionId;
        this.connectionId = NO_CONNECTION_ID;
        endOperation();
        return connectionId;
    }

    public void idToLibrary(final Int2ObjectHashMap<LiveLibraryInfo> idToLibrary)
    {
        this.idToLibrary = idToLibrary;
    }

    private void startOperation()
    {
        System.out.println("ReproductionProtocolHandler.startOperation: " + operationInProgress);
        if (operationInProgress)
        {
            errorHandler.onError(new IllegalStateException("Multiple operations in flight attempted"));
        }

        operationInProgress = true;
    }

    private void endOperation()
    {
        System.out.println("ReproductionProtocolHandler.endOperation: " + operationInProgress);
        if (!operationInProgress)
        {
            errorHandler.onError(new IllegalStateException("No operation in flight"));
        }

        operationInProgress = false;
    }

    public boolean operationInProgress()
    {
        return operationInProgress;
    }
}
