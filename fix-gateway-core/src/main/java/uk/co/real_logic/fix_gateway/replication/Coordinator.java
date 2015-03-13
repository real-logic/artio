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
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.messages.FixMessage;
import uk.co.real_logic.fix_gateway.messages.MessageAcknowledgement;
import uk.co.real_logic.fix_gateway.messages.MessageHeader;
import uk.co.real_logic.fix_gateway.util.Long2LongHashMap;

import static uk.co.real_logic.fix_gateway.messages.MessageAcknowledgement.SCHEMA_VERSION;

public class Coordinator implements Agent
{
    public static final int NO_SESSION_ID = -1;

    private final MessageHeader messageHeader = new MessageHeader();
    private final MessageAcknowledgement messageAcknowledgement = new MessageAcknowledgement();
    private final FixMessage fixMessage = new FixMessage();

    private final MessageHandler delegate;
    private final TermAcknowledgementStrategy termAcknowledgementStrategy;

    private final Subscription dataSubscription;
    private final Publication controlPublication;
    private final Subscription controlSubscription;

    private final Long2LongHashMap sessionToAckedTerms = new Long2LongHashMap(NO_SESSION_ID);
    private long acknowledgedTerm = 0;

    // Counts of how many acknowledgements

    public Coordinator(
        final ReplicationStreams replicationStreams,
        final MessageHandler delegate,
        final IntHashSet followers,
        final TermAcknowledgementStrategy termAcknowledgementStrategy)
    {
        this.delegate = delegate;
        this.termAcknowledgementStrategy = termAcknowledgementStrategy;

        dataSubscription = replicationStreams.dataSubscription(this::onDataMessage);
        controlPublication = replicationStreams.controlPublication();
        controlSubscription = replicationStreams.controlSubscription(this::onControlMessage);

        followers.forEach(follower -> sessionToAckedTerms.put(follower, 0));
    }

    private void onDataMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final UnsafeBuffer unsafeBuffer = (UnsafeBuffer) buffer;
        fixMessage.wrapForDecode(unsafeBuffer, offset, length, SCHEMA_VERSION);

        final long fixSessionId = fixMessage.session();
        delegate.onMessage(buffer, offset, length, fixSessionId);
    }

    private void onControlMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final UnsafeBuffer unsafeBuffer = (UnsafeBuffer) buffer;
        if (isTemplate(offset, unsafeBuffer, MessageAcknowledgement.TEMPLATE_ID))
        {
            messageAcknowledgement.wrapForDecode(unsafeBuffer, offset, length, SCHEMA_VERSION);
            onMessageAcknowledgement(messageAcknowledgement.term(), header.sessionId());
        }
    }

    private boolean isTemplate(final int offset, final UnsafeBuffer unsafeBuffer, final int templateId)
    {
        messageHeader.wrap(unsafeBuffer, offset, 0);
        return messageHeader.templateId() == templateId;
    }

    public void onMessageAcknowledgement(final int newAckedterm, final int session)
    {
        final long lastAckedTerm = sessionToAckedTerms.get(session);
        if (lastAckedTerm != NO_SESSION_ID)
        {
            if (newAckedterm > lastAckedTerm)
            {
                sessionToAckedTerms.put(session, newAckedterm);

                final long newAcknowledgedTerm = termAcknowledgementStrategy.findAckedTerm(sessionToAckedTerms);
                if (newAcknowledgedTerm > acknowledgedTerm)
                {
                    // TODO: dataSubscription.pollToPosition(newAcknowledgedTerm);
                    acknowledgedTerm = newAcknowledgedTerm;

                    // TODO: broadcast to followers that the message was committed to the delegate
                }
            }
        }
        else
        {
            // TODO: error case
        }
    }

    public int doWork() throws Exception
    {
        // TODO: some batch
        return controlSubscription.poll(10);
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
