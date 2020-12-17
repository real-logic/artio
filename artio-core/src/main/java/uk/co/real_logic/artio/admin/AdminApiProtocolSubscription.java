/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.admin;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.messages.AllFixSessionsReplyDecoder;
import uk.co.real_logic.artio.messages.DisconnectSessionReplyDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

class AdminApiProtocolSubscription implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final AllFixSessionsReplyDecoder allFixSessionsReply = new AllFixSessionsReplyDecoder();
    private final DisconnectSessionReplyDecoder disconnectSessionReply = new DisconnectSessionReplyDecoder();

    private final AdminEndPointHandler handler;

    AdminApiProtocolSubscription(final AdminEndPointHandler handler)
    {
        this.handler = handler;
    }

    @SuppressWarnings("FinalParameters")
    public void onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        final MessageHeaderDecoder messageHeader = this.messageHeader;
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case AllFixSessionsReplyDecoder.TEMPLATE_ID:
            {
                onAllFixSessionsReply(buffer, offset, blockLength, version);
                return;
            }

            case DisconnectSessionReplyDecoder.TEMPLATE_ID:
            {
                onDisconnectSessionReply(buffer, offset, blockLength, version);
                return;
            }
        }
    }

    private void onAllFixSessionsReply(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        final AllFixSessionsReplyDecoder allFixSessionsReply = this.allFixSessionsReply;
        allFixSessionsReply.wrap(buffer, offset, blockLength, version);

        handler.onAllFixSessionsReply(
            allFixSessionsReply.correlationId(),
            allFixSessionsReply.sessions());
    }

    private void onDisconnectSessionReply(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        final DisconnectSessionReplyDecoder disconnectSessionReply = this.disconnectSessionReply;
        disconnectSessionReply.wrap(buffer, offset, blockLength, version);

        handler.onDisconnectSessionReply(
            disconnectSessionReply.correlationId(),
            disconnectSessionReply.errorType(),
            disconnectSessionReply.message());
    }
}
