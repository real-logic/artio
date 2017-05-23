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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import uk.co.real_logic.fix_gateway.DebugLogger;

import static uk.co.real_logic.fix_gateway.LogTag.RAFT;

public class FrameDropper
{
    private final SwitchableLossGenerator outboundLossGenerator = new SwitchableLossGenerator();
    private final SwitchableLossGenerator inboundLossGenerator = new SwitchableLossGenerator();
    private final int nodeId;

    public FrameDropper(final int nodeId)
    {
        this.nodeId = nodeId;
    }

    public SendChannelEndpointSupplier newSendChannelEndpointSupplier()
    {
        return (udpChannel, statusIndicator, context) ->
            new DebugSendChannelEndpoint(
                udpChannel, statusIndicator, context, outboundLossGenerator, outboundLossGenerator);
    }

    public ReceiveChannelEndpointSupplier newReceiveChannelEndpointSupplier()
    {
        return (udpChannel, dispatcher, statusIndicator, context) ->
            new DebugReceiveChannelEndpoint(
                udpChannel, dispatcher, statusIndicator, context, inboundLossGenerator, inboundLossGenerator);
    }

    public void dropFrames(final boolean dropFrames)
    {
        dropFrames(dropFrames, dropFrames);
    }

    public void dropFrames(final boolean dropInboundFrames, final boolean dropOutboundFrames)
    {
        DebugLogger.log(RAFT, "Dropping frames to %d: %b%n", nodeId, dropInboundFrames);
        DebugLogger.log(RAFT, "Dropping frames from %d: %b%n", nodeId, dropOutboundFrames);
        inboundLossGenerator.dropFrames(dropInboundFrames);
        outboundLossGenerator.dropFrames(dropOutboundFrames);
    }
}
