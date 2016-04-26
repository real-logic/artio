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

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;

class SenderEndPoints implements AutoCloseable, ControlledFragmentHandler
{
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final Long2ObjectHashMap<SenderEndPoint> connectionIdToSenderEndpoint = new Long2ObjectHashMap<>();

    private GatewayPublication publication;

    SenderEndPoints(final GatewayPublication publication)
    {
        this.publication = publication;
    }

    public void add(final SenderEndPoint senderEndPoint)
    {
        connectionIdToSenderEndpoint.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    public void removeConnection(final long connectionId)
    {
        final SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.remove(connectionId);
        if (senderEndPoint != null)
        {
            senderEndPoint.close();
        }
    }

    public void onMessage(
        final long connectionId, final DirectBuffer buffer, final int offset, final int length)
    {
        final SenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onNormalFramedMessage(buffer, offset, length);
        }
    }

    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        if (messageHeader.templateId() == FixMessageDecoder.TEMPLATE_ID)
        {
            offset += HEADER_LENGTH;
            fixMessage.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());
            final long connectionId = fixMessage.connection();

            final SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
            if (senderEndPoint != null)
            {
                return senderEndPoint.onQuarantinedMessageFragment(fixMessage, buffer, offset, length - HEADER_LENGTH);
            }
        }

        return Action.CONTINUE;
    }

    public void close()
    {
        connectionIdToSenderEndpoint
            .values()
            .forEach(SenderEndPoint::close);
    }
}
