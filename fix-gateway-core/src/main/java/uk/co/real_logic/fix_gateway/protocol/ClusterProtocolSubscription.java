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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.DisconnectDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.ClusterAgent;
import uk.co.real_logic.fix_gateway.replication.ClusterPublication;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class ClusterProtocolSubscription implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final BufferClaim bufferClaim = new BufferClaim();

    private final ClusterPublication publication;
    private final ClusterAgent node;
    private final ControlledFragmentHandler other;

    public ClusterProtocolSubscription(
        final ClusterPublication publication,
        final ClusterAgent node,
        final ControlledFragmentHandler other)
    {
        this.publication = publication;
        this.node = node;
        this.other = other;
    }

    public Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int templateId = messageHeader.wrap(buffer, offset).templateId();

        switch (templateId)
        {
            case FixMessageDecoder.TEMPLATE_ID:
            case DisconnectDecoder.TEMPLATE_ID:
            {
                if (node.isLeader())
                {
                    final long position = publication.tryClaim(length, bufferClaim);
                    if (position < 0)
                    {
                        return ABORT;
                    }

                    bufferClaim.buffer().putBytes(bufferClaim.offset(), buffer, offset, length);

                    bufferClaim.commit();
                    return CONTINUE;
                }
                else
                {
                    // TODO: log number of messages dropped
                    return CONTINUE;
                }
            }

            default:
                return other.onFragment(buffer, offset, length, header);
        }
    }
}
