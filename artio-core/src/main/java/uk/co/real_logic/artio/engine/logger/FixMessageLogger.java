/*
 * Copyright 2015-2019 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Aeron;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.Verify;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.ilink.ILinkMessageConsumer;
import uk.co.real_logic.artio.messages.FixMessageDecoder;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_OUTBOUND_REPLAY_STREAM;

/**
 * Prints out FIX messages from an Aeron Stream - designed for integration into logging tools like
 * Splunk.
 *
 * Main method is provided as an example of usage - when integrating into your specific system you should pass in the
 * library aeron channel and stream ids used by your {@link uk.co.real_logic.artio.engine.EngineConfiguration}.
 *
 * Since this class generates Java objects for every message that passes through the system you're recommended to run
 * it in a different process to the normal Artio Engine if you're operating in a latency sensitive environment.
 */
public class FixMessageLogger implements Agent
{
    public static class Configuration
    {
        private static final int DEFAULT_COMPACTION_SIZE = 64 * 1024;

        private FixMessageConsumer fixMessageConsumer;
        private Aeron.Context context;
        private String libraryAeronChannel = IPC_CHANNEL;
        private int inboundStreamId = DEFAULT_INBOUND_LIBRARY_STREAM;
        private int outboundStreamId = DEFAULT_OUTBOUND_LIBRARY_STREAM;
        private int outboundReplayStreamId = DEFAULT_OUTBOUND_REPLAY_STREAM;
        private int compactionSize = DEFAULT_COMPACTION_SIZE;
        private ILinkMessageConsumer iLinkMessageConsumer;

        public Configuration fixMessageConsumer(final FixMessageConsumer fixMessageConsumer)
        {
            this.fixMessageConsumer = fixMessageConsumer;
            return this;
        }

        public Configuration iLinkMessageConsumer(final ILinkMessageConsumer iLinkMessageConsumer)
        {
            this.iLinkMessageConsumer = iLinkMessageConsumer;
            return this;
        }

        public Configuration context(final Aeron.Context context)
        {
            this.context = context;
            return this;
        }

        public Configuration libraryAeronChannel(final String libraryAeronChannel)
        {
            this.libraryAeronChannel = libraryAeronChannel;
            return this;
        }

        public Configuration inboundStreamId(final int inboundStreamId)
        {
            this.inboundStreamId = inboundStreamId;
            return this;
        }

        public Configuration outboundStreamId(final int outboundStreamId)
        {
            this.outboundStreamId = outboundStreamId;
            return this;
        }

        public Configuration outboundReplayStreamId(final int outboundReplayStreamId)
        {
            this.outboundReplayStreamId = outboundReplayStreamId;
            return this;
        }

        public Configuration compactionSize(final int compactionSize)
        {
            this.compactionSize = compactionSize;
            return this;
        }

        void conclude()
        {
            Verify.notNull(fixMessageConsumer, "fixMessageConsumer");

            if (compactionSize <= 0)
            {
                throw new IllegalArgumentException("Compaction size must be positive, but is: " + compactionSize);
            }

            if (context == null)
            {
                context = new Aeron.Context();
            }
        }
    }

    public static void main(final String[] args)
    {
        final Configuration configuration = new Configuration()
            .fixMessageConsumer(FixMessageLogger::print);
        final FixMessageLogger logger = new FixMessageLogger(configuration);

        final AgentRunner runner = new AgentRunner(
            CommonConfiguration.backoffIdleStrategy(),
            Throwable::printStackTrace,
            null,
            logger);

        AgentRunner.startOnThread(runner);
    }

    private static void print(
        final FixMessageDecoder fixMessageDecoder,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        System.out.printf("%s: %s%n", fixMessageDecoder.status(), fixMessageDecoder.body());
    }

    private final StreamTimestampZipper zipper;
    private final Aeron aeron;

    @Deprecated
    public FixMessageLogger(
        final FixMessageConsumer fixMessageConsumer,
        final Aeron.Context context,
        final String libraryAeronChannel,
        final int inboundStreamId,
        final int outboundStreamId,
        final int outboundReplayStreamId)
    {
        this(new Configuration()
            .fixMessageConsumer(fixMessageConsumer)
            .context(context)
            .libraryAeronChannel(libraryAeronChannel)
            .inboundStreamId(inboundStreamId)
            .outboundStreamId(outboundStreamId)
            .outboundReplayStreamId(outboundReplayStreamId));
    }

    public FixMessageLogger(
        final Configuration configuration)
    {
        configuration.conclude();
        aeron = Aeron.connect(configuration.context);
        zipper = new StreamTimestampZipper(
            aeron,
            configuration.libraryAeronChannel,
            configuration.fixMessageConsumer,
            configuration.iLinkMessageConsumer,
            configuration.compactionSize,
            configuration.inboundStreamId,
            configuration.outboundStreamId,
            configuration.outboundReplayStreamId);
    }

    public int doWork()
    {
        return zipper.poll();
    }

    public void onClose()
    {
        aeron.close();
    }

    public String roleName()
    {
        return "FixMessageLogger";
    }

    int bufferPosition()
    {
        return zipper.bufferPosition();
    }

    int bufferCapacity()
    {
        return zipper.bufferCapacity();
    }
}
