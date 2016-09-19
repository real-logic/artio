/*
 * Copyright 2014 Real Logic Ltd.
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

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.IntIterator;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.ArchivedPositionHandler;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;

// TODO: identify how to improve liveness in the situation that no messages
// are replicated for a long term once a stream has been replicated.
class ClusterPositionSender implements Agent, ArchivedPositionHandler
{
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int LIMIT = 10;

    private static final int MISSING = -1;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final ReplicatedMessageDecoder replicatedMessage = new ReplicatedMessageDecoder();
    private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();

    private final Long2LongHashMap libraryIdToClusterPosition = new Long2LongHashMap(MISSING);
    private final Long2LongHashMap libraryIdToArchivedPosition = new Long2LongHashMap(MISSING);
    private final Long2LongHashMap aeronSessionIdToArchivedPosition = new Long2LongHashMap(MISSING);
    private final Int2IntHashMap aeronSessionIdToLibraryId = new Int2IntHashMap(MISSING);
    private final IntHashSet updatedLibraryIds = new IntHashSet(MISSING);

    private final ClusterableSubscription outboundLibrarySubscription;
    private final ControlledFragmentHandler onLibraryFragmentFunc = this::onLibraryFragment;
    private final ClusterableSubscription outboundClusterSubscription;
    private final ControlledFragmentHandler onClusterFragmentFunc = this::onClusterFragment;
    private final GatewayPublication inboundLibraryPublication;

    ClusterPositionSender(
        final ClusterableSubscription outboundLibrarySubscription,
        final ClusterableSubscription outboundClusterSubscription,
        final GatewayPublication inboundLibraryPublication)
    {
        this.outboundLibrarySubscription = outboundLibrarySubscription;
        this.outboundClusterSubscription = outboundClusterSubscription;
        this.inboundLibraryPublication = inboundLibraryPublication;
    }

    public int doWork() throws Exception
    {
        return pollCommands() + checkConditions();
    }

    private int pollCommands()
    {
        return outboundLibrarySubscription.controlledPoll(onLibraryFragmentFunc, LIMIT) +
               outboundClusterSubscription.controlledPoll(onClusterFragmentFunc, LIMIT);
    }

    private Action onLibraryFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        offset += HEADER_LENGTH;

        if (messageHeader.templateId() == LibraryConnectDecoder.TEMPLATE_ID)
        {
            libraryConnect.wrap(
                buffer, offset, messageHeader.blockLength(), messageHeader.version());
            onLibraryConnect(header.sessionId(), libraryConnect.libraryId());
        }

        return Action.CONTINUE;
    }

    private Action onClusterFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        offset = wrapHeader(buffer, offset);

        if (messageHeader.templateId() == ReplicatedMessageDecoder.TEMPLATE_ID)
        {
            replicatedMessage.wrap(
                buffer, offset, messageHeader.blockLength(), messageHeader.blockLength());

            final long position = replicatedMessage.position();

            offset += ReplicatedMessageDecoder.BLOCK_LENGTH;

            offset = wrapHeader(buffer, offset);

            final int version = messageHeader.version();
            final int actingBlockLength = messageHeader.blockLength();

            switch (messageHeader.templateId())
            {
                case DisconnectDecoder.TEMPLATE_ID:
                {
                    disconnect.wrap(buffer, offset, actingBlockLength, version);
                    final int libraryId = disconnect.libraryId();
                    onClusteredLibraryPosition(libraryId, position);
                    break;
                }

                case FixMessageDecoder.TEMPLATE_ID:
                {
                    fixMessage.wrap(buffer, offset, actingBlockLength, version);
                    final int libraryId = fixMessage.libraryId();
                    onClusteredLibraryPosition(libraryId, position);
                    break;
                }
            }
        }

        return Action.CONTINUE;
    }

    private int wrapHeader(final DirectBuffer buffer, int offset)
    {
        messageHeader.wrap(buffer, offset);
        offset += MessageHeaderDecoder.ENCODED_LENGTH;
        return offset;
    }

    int checkConditions()
    {
        int resendCount = 0;
        final IntIterator it = updatedLibraryIds.iterator();
        while (it.hasNext())
        {
            final int libraryId = it.nextValue();

            long position = libraryIdToArchivedPosition.get(libraryId);
            if (position != MISSING)
            {
                final long clusterPosition = libraryIdToClusterPosition.get(libraryId);
                if (clusterPosition != MISSING)
                {
                    position = Math.min(position, clusterPosition);
                }

                if (inboundLibraryPublication.saveNewSentPosition(libraryId, position) >= 0)
                {
                    resendCount++;
                }
                else
                {
                    continue; // don't remove libraryId
                }
            }

            // TODO: move to it.remove() in agrona 0.5.5
            updatedLibraryIds.remove(libraryId);
        }

        return resendCount;
    }

    public String roleName()
    {
        return "ClusterPositionSender";
    }

    void onLibraryConnect(final int sessionId, final int libraryId)
    {
        aeronSessionIdToLibraryId.put(sessionId, libraryId);
        // Backup path to avoid missing a position
        final long archivedPosition = aeronSessionIdToArchivedPosition.remove(sessionId);
        if (archivedPosition != MISSING)
        {
            libraryIdToArchivedPosition.put(libraryId, archivedPosition);
            updatedLibraryIds.add(libraryId);
        }
    }

    void onClusteredLibraryPosition(final int libraryId, final long position)
    {
        libraryIdToClusterPosition.put(libraryId, position);
    }

    public void onArchivedPosition(final int aeronSessionId, final long position)
    {
        final int libraryId = aeronSessionIdToLibraryId.get(aeronSessionId);
        if (libraryId != MISSING)
        {
            libraryIdToArchivedPosition.put(libraryId, position);
            updatedLibraryIds.add(libraryId);
        }
        else
        {
            // Backup path in case we haven't yet seen the library connect message
            aeronSessionIdToArchivedPosition.put(aeronSessionId, position);
        }
    }
}
