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
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.EngineDescriptorDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.NodeStateHandler;

public class EngineDescriptorStore implements NodeStateHandler
{
    // Thread-safe state
    private volatile UnsafeBuffer leaderLibraryChannel = null;
    private Integer leaderSessionId;

    // Single-threaded state only used on archiver thread.
    private final Int2ObjectHashMap<UnsafeBuffer> nodeIdToLibraryChannel = new Int2ObjectHashMap<>();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final EngineDescriptorDecoder engineDescriptor = new EngineDescriptorDecoder();

    private final ErrorHandler errorHandler;

    EngineDescriptorStore(final ErrorHandler errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public void onNewNodeState(
        final short nodeId,
        final int aeronSessionId,
        final DirectBuffer buffer,
        final int nodeStateLength)
    {
        int offset = 0;
        messageHeader.wrap(buffer, offset);

        final int templateId = messageHeader.templateId();
        if (templateId != EngineDescriptorDecoder.TEMPLATE_ID)
        {
            errorHandler.onError(
                new IllegalArgumentException(
                    String.format(
                        "Invalid template Id, expected %d, but received %d from %d",
                        EngineDescriptorDecoder.TEMPLATE_ID,
                        templateId,
                        nodeId)));
            return;
        }

        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        engineDescriptor
            .wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

        final int libraryChannelLength = engineDescriptor.libraryChannelLength();
        final UnsafeBuffer libraryChannel = new UnsafeBuffer(new byte[libraryChannelLength]);
        engineDescriptor.getLibraryChannel(libraryChannel, 0, libraryChannelLength);
        nodeIdToLibraryChannel.put(aeronSessionId, libraryChannel);
        updateLeaderLibraryChannel();
    }

    public void onNewLeader(final int leaderSessionId)
    {
        this.leaderSessionId = leaderSessionId;
        updateLeaderLibraryChannel();
    }

    public void noLeader()
    {
        leaderSessionId = null;
        leaderLibraryChannel = null;
    }

    private void updateLeaderLibraryChannel()
    {
        if (leaderSessionId != null)
        {
            leaderLibraryChannel = nodeIdToLibraryChannel.get(leaderSessionId + 2);
        }
    }

    public DirectBuffer leaderLibraryChannel()
    {
        return leaderLibraryChannel;
    }
}
