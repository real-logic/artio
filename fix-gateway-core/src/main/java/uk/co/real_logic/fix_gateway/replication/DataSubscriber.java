/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.DisconnectDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.session.SessionHandler;

import static uk.co.real_logic.fix_gateway.replication.GatewayPublication.FRAME_SIZE;

public class DataSubscriber implements DataHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private SessionHandler sessionHandler;

    public void onData(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        offset += messageHeader.size();

        switch (messageHeader.templateId())
        {
            case FixMessageDecoder.TEMPLATE_ID:
            {
                messageFrame.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());
                final int messageLength = length - (FRAME_SIZE + messageHeader.size());
                sessionHandler.onMessage(
                    buffer,
                    offset + FRAME_SIZE,
                    messageLength,
                    messageFrame.connection(),
                    messageFrame.session(),
                    messageFrame.messageType());
                break;
            }

            case DisconnectDecoder.TEMPLATE_ID:
            {
                disconnect.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());
                final long connectionId = disconnect.connection();
                DebugLogger.log("FixSubscription Disconnect: %d\n", connectionId);
                sessionHandler.onDisconnect(connectionId);
                break;
            }
        }
    }

    public DataSubscriber sessionHandler(final SessionHandler sessionHandler)
    {
        this.sessionHandler = sessionHandler;
        return this;
    }
}
