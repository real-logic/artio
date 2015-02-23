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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.util.IntHashSet;

public class Coordinator implements Agent
{
    // TODO: size appropriately
    private static final int MAX_UNACKNOWLEDGED_TERMS = 10;

    private final MessageHandler delegate;
    private final IntHashSet followers;

    private final Subscription dataSubscription;
    private final Publication controlPublication;
    private final Subscription controlSubscription;

    // Counts of how many acknowledgements
    private final int[] acknowledgementCounts = new int[MAX_UNACKNOWLEDGED_TERMS];
    private int acknowledgedTermId = 0;

    public Coordinator(
        final ReplicationStreams replicationStreams,
        final MessageHandler delegate,
        final IntHashSet followers)
    {
        this.delegate = delegate;
        this.followers = followers;

        dataSubscription = replicationStreams.dataSubscription(this::onDataMessage);
        controlPublication = replicationStreams.controlPublication();
        controlSubscription = replicationStreams.controlSubscription(this::onControlMessage);
    }

    private void onDataMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int termId = header.termId();
        while (acknowledgedTermId < termId)
        {
            if (controlSubscription.poll(1) != 1)
            {
                // TODO: backoff
            }
        }

        // TODO: parse from framing message
        final int connectionId = -1;

        delegate.onMessage(buffer, offset, length, connectionId);

        // TODO: broadcast to followers that the message was committed to the delegate
    }

    private void onControlMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // TODO: parse message
        int termId = -1;
        onMessageAcknowledgement(termId, header.sessionId());
    }

    private void onMessageAcknowledgement(final int termId, final int sessionId)
    {
        if (followers.contains(sessionId))
        {
            // TODO: update to be a sessionId -> max position map - Long2LongHashMap
            // TODO: apply strategy to get the new acknowledged term id
            // TODO: read from data stream up to acknowledged term id
            // TODO: add pollToPosition(termId);

            if (termId < acknowledgedTermId)
            {
                // their vote was too slow to matter.
                return;
            }

            if (termId > acknowledgedTermId + MAX_UNACKNOWLEDGED_TERMS)
            {
                // TODO: error - we've fallen behind, the cluster is running too slowly.
            }

            // TODO: figure out if the message should be a batch update, or a single acknowledgement
            acknowledgementCounts[index(termId)]++;

            // scan and update the acknowledgedTermId
            while (true)
            {
                final int termIermIndex = index(acknowledgedTermId + 1);
                final int countAtNextTermId = acknowledgementCounts[termIermIndex];
                // TODO: strategy
                final boolean sufficientAcknowledgements = countAtNextTermId == followers.size();
                if (sufficientAcknowledgements)
                {
                    acknowledgedTermId++;
                    acknowledgementCounts[termIermIndex] = 0;
                }
                else
                {
                    break;
                }
            }
        }
        else
        {
            // TODO: error case
        }
    }

    private boolean acknowledgedNextTerm()
    {
        final int termIermIndex = index(acknowledgedTermId + 1);
        final int countAtNextTermId = acknowledgementCounts[termIermIndex];
        // TODO: strategy
        final boolean haveUpdated = countAtNextTermId == followers.size();
        return haveUpdated;
    }

    private int index(final int termId)
    {
        return termId % MAX_UNACKNOWLEDGED_TERMS;
    }

    public int doWork() throws Exception
    {
        // TODO: some batch
        return controlSubscription.poll(10);
        //return dataSubscription.poll(1);
    }

    public void onClose()
    {
        dataSubscription.close();
        controlPublication.close();
        controlSubscription.close();
    }

    public String roleName()
    {
        return "Coordinator";
    }
}
