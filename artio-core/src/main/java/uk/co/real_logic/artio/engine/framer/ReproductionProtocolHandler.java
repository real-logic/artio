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
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.engine.EngineReproductionClock;
import uk.co.real_logic.artio.engine.logger.ReproductionFixProtocolConsumer;
import uk.co.real_logic.artio.messages.ApplicationHeartbeatDecoder;
import uk.co.real_logic.artio.messages.ConnectDecoder;
import uk.co.real_logic.artio.messages.FixMessageDecoder;

import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;

public class ReproductionProtocolHandler implements ReproductionFixProtocolConsumer
{
    // Decode protocol for relevant messages and hand them off down the line to
    // Artio component that is responsible for dealing with the event in normal times.
    // components should block until operation complete to ensure ordering is maintained.
    // TODO: need a way of controlling the sending of application heartbeats as well

    private final ReproductionTcpChannelSupplier tcpChannelSupplier;
    private final EngineReproductionClock clock;

    private long connectionId = NO_CONNECTION_ID;

    public ReproductionProtocolHandler(
        final ReproductionTcpChannelSupplier tcpChannelSupplier,
        final EngineReproductionClock clock)
    {
        this.tcpChannelSupplier = tcpChannelSupplier;
        this.clock = clock;
    }

    public void onMessage(
        final FixMessageDecoder message,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ArtioLogHeader header)
    {
        System.out.println("ReproductionProtocolHandler.onMessage");
        clock.advanceTimeTo(message.timestamp());
    }

    public void onConnect(
        final ConnectDecoder connectDecoder,
        final DirectBuffer buffer,
        final int start,
        final int length)
    {
        System.out.println("ReproductionProtocolHandler.onConnect");
        clock.advanceTimeTo(connectDecoder.timestamp());
        connectionId = connectDecoder.connection();
        tcpChannelSupplier.enqueueConnect(connectDecoder);
    }

    public void onApplicationHeartbeat(
        final ApplicationHeartbeatDecoder decoder, final DirectBuffer buffer, final int start, final int length)
    {
        System.out.println("ReproductionProtocolHandler.onApplicationHeartbeat");
        clock.advanceTimeTo(decoder.timestampInNs());
    }

    public long newConnectionId()
    {
        System.out.println("ReproductionProtocolHandler.newConnectionId");
        if (connectionId == NO_CONNECTION_ID)
        {
            throw new IllegalStateException("Unknown connection id");
        }

        final long connectionId = this.connectionId;
        this.connectionId = NO_CONNECTION_ID;
        return connectionId;
    }
}
