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
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import uk.co.real_logic.artio.CommonConfiguration;

import java.util.function.Consumer;

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
    public static void main(final String[] args)
    {
        final FixMessageLogger logger = new FixMessageLogger(
            System.out::println,
            new Aeron.Context(),
            IPC_CHANNEL,
            DEFAULT_INBOUND_LIBRARY_STREAM,
            DEFAULT_OUTBOUND_LIBRARY_STREAM,
            DEFAULT_OUTBOUND_REPLAY_STREAM);

        final AgentRunner runner = new AgentRunner(
            CommonConfiguration.backoffIdleStrategy(),
            Throwable::printStackTrace,
            null,
            logger
        );

        AgentRunner.startOnThread(runner);
    }

    private final Aeron aeron;
    private final Subscription outboundSubscription;
    private final Subscription inboundSubscription;
    private final Subscription replaySubscription;
    private final FragmentAssembler fragmentAssembler;

    public FixMessageLogger(
        final Consumer<String> fixMessageConsumer,
        final Aeron.Context context,
        final String libraryAeronChannel,
        final int inboundStreamId,
        final int outboundStreamId,
        final int outboundReplayStreamId)
    {
        aeron = Aeron.connect(context);
        inboundSubscription = aeron.addSubscription(libraryAeronChannel, inboundStreamId);
        outboundSubscription = aeron.addSubscription(libraryAeronChannel, outboundStreamId);
        replaySubscription = aeron.addSubscription(libraryAeronChannel, outboundReplayStreamId);

        final LogEntryHandler logEntryHandler = new LogEntryHandler((message, buffer, offset, length, header) ->
            fixMessageConsumer.accept(message.body()));
        fragmentAssembler = new FragmentAssembler(logEntryHandler);
    }

    public int doWork()
    {
        return
            inboundSubscription.poll(fragmentAssembler, 10) +
            outboundSubscription.poll(fragmentAssembler, 10) +
            replaySubscription.poll(fragmentAssembler, 10);
    }

    public void onClose()
    {
        aeron.close();
    }

    public String roleName()
    {
        return "FixMessageLogger";
    }
}
