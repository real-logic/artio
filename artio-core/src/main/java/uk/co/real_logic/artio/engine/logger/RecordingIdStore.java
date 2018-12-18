/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.MessageHeaderDecoder;
import io.aeron.archive.codecs.RecordingStartedDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;

import java.nio.charset.StandardCharsets;

import static io.aeron.archive.codecs.RecordingStartedDecoder.channelHeaderLength;

public class RecordingIdStore implements AutoCloseable
{
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    private final RecordingIdLookup inboundLookup;
    private final RecordingIdLookup outboundLookup;
    private final Subscription subscription;
    private final RecordingEventHandler recordingEventHandler;

    public RecordingIdStore(
        final Aeron aeron,
        final AeronArchive.Context archiveContext,
        final String requiredChannel,
        final AgentInvoker conductorAgentInvoker,
        final IdleStrategy startupIdleStrategy,
        final IdleStrategy archiverIdleStrategy,
        final int inboundLibraryStream,
        final int outboundLibraryStream)
    {
        subscription = aeron.addSubscription(
            archiveContext.recordingEventsChannel(),
            archiveContext.recordingEventsStreamId());

        inboundLookup = new RecordingIdLookup(inboundLibraryStream, this, archiverIdleStrategy);
        outboundLookup = new RecordingIdLookup(outboundLibraryStream, this, archiverIdleStrategy);

        recordingEventHandler = new RecordingEventHandler(inboundLookup, outboundLookup, requiredChannel);

        // Wait for the subscription setup to ensure that we receive onStart events of all
        // recording started after this constructor.
        while (!subscription.isConnected())
        {
            poll();

            if (conductorAgentInvoker != null)
            {
                conductorAgentInvoker.invoke();
            }

            startupIdleStrategy.idle();
        }

        startupIdleStrategy.reset();
    }

    public RecordingIdLookup inboundLookup()
    {
        return inboundLookup;
    }

    public RecordingIdLookup outboundLookup()
    {
        return outboundLookup;
    }

    int poll()
    {
        return subscription.poll(recordingEventHandler, FRAGMENT_COUNT_LIMIT);
    }

    @Override
    public void close()
    {
        subscription.close();
    }

    // RecordingEventsHandler that ships with Aeron Archive allocates objects for every onStart event.
    static class RecordingEventHandler implements FragmentHandler
    {
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private final RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();

        private final byte[] requiredChannel;
        private final RecordingIdLookup inboundLookup;
        private final RecordingIdLookup outboundLookup;

        RecordingEventHandler(
            final RecordingIdLookup inboundLookup,
            final RecordingIdLookup outboundLookup,
            final String requiredChannel)
        {
            this.inboundLookup = inboundLookup;
            this.outboundLookup = outboundLookup;
            this.requiredChannel = requiredChannel.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public void onFragment(
            final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messageHeaderDecoder.wrap(buffer, offset);

            final int templateId = messageHeaderDecoder.templateId();
            switch (templateId)
            {
                case RecordingStartedDecoder.TEMPLATE_ID:
                    recordingStartedDecoder.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());

                    final long recordingId = recordingStartedDecoder.recordingId();
                    final int sessionId = recordingStartedDecoder.sessionId();
                    final int streamId = recordingStartedDecoder.streamId();

                    if (hasRequiredChannel(buffer))
                    {
                        inboundLookup.onStart(recordingId, sessionId, streamId);
                        outboundLookup.onStart(recordingId, sessionId, streamId);
                    }

                    break;
            }
        }

        private boolean hasRequiredChannel(final DirectBuffer buffer)
        {
            final byte[] requiredChannel = this.requiredChannel;
            final int channelLength = recordingStartedDecoder.channelLength();
            if (channelLength != requiredChannel.length)
            {
                return false;
            }

            final int channelOffset = recordingStartedDecoder.limit() + channelHeaderLength();

            for (int i = 0; i < channelLength; i++)
            {
                if (requiredChannel[i] != buffer.getByte(i + channelOffset))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
