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
package uk.co.real_logic.fix_gateway.engine;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.EngineDescriptorDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.NodeStateHandler;

public class EngineDescriptorStore implements NodeStateHandler
{
    // Thread-safe state
    private volatile UnsafeBuffer leaderLibraryChannel = null;

    // Single-threaded state only used on archiver thread.
    private final Int2ObjectHashMap<UnsafeBuffer> nodeIdToLibraryChannel = new Int2ObjectHashMap<>();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final EngineDescriptorDecoder engineDescriptor = new EngineDescriptorDecoder();

    public void onNewNodeState(final short nodeId, final DirectBuffer buffer, int offset)
    {
        messageHeader.wrap(buffer, offset);
        if (messageHeader.templateId() != EngineDescriptorDecoder.TEMPLATE_ID)
        {
            // TODO: log error
            return;
        }

        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        engineDescriptor
            .wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

        final int libraryChannelLength = engineDescriptor.libraryChannelLength();
        final UnsafeBuffer libraryChannel = new UnsafeBuffer(new byte[libraryChannelLength]);
        engineDescriptor.getLibraryChannel(libraryChannel, 0, libraryChannelLength);
        nodeIdToLibraryChannel.put(Short.valueOf(nodeId), libraryChannel);
    }

    public DirectBuffer leaderLibraryChannel()
    {
        return leaderLibraryChannel;
    }
}
