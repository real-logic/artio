/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.engine.ReproductionMessageHandler;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.Constants.NEW_ORDER_SINGLE_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.closeMediaDriver;
import static uk.co.real_logic.artio.library.FixLibrary.CURRENT_SEQUENCE;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ReproductionSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
    public static final int MESSAGES_SENT = 3;

    static class StashingMessageHandler implements ReproductionMessageHandler
    {
        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        public void onMessage(final long connectionId, final ByteBuffer bytes)
        {
            final byte[] stashed = new byte[bytes.remaining()];
            bytes.get(stashed);
            final String message = new String(stashed, StandardCharsets.US_ASCII);
            System.out.println("message = " + message);
            messages.add(message);
        }

        public List<String> messages()
        {
            return Collections.unmodifiableList(messages);
        }
    }

    @Test
    public void shouldReproduceMessageExchange() throws IOException
    {
        final ReportFactory reportFactory = new ReportFactory();
        final List<FixMessage> originalReceivedMessages = new ArrayList<>();
        final long[] sentPositions = new long[MESSAGES_SENT];
        final List<String> sentMessages = new ArrayList<>();

        final long startInNs = nanoClock.nanoTime();
        createScenario(reportFactory, originalReceivedMessages, sentPositions, sentMessages);
        final long endInNs = nanoClock.nanoTime();

        // TODO: logon and perform a replay

        reproduceScenario(reportFactory, originalReceivedMessages, sentPositions, sentMessages, startInNs, endInNs);
    }

    private void reproduceScenario(
        final ReportFactory reportFactory,
        final List<FixMessage> originalReceivedMessages,
        final long[] sentPositions,
        final List<String> sentMessages,
        final long startInNs, final long endInNs)
    {
        final StashingMessageHandler messageStash = new StashingMessageHandler();
        reproductionMessageHandler = messageStash;

        setup(false, true, true, InitialAcceptedSessionOwner.ENGINE, false,
            true, startInNs, endInNs, false);
        setupLibrary();

        // Reply to messages
        final long[] reproPositions = new long[MESSAGES_SENT];
        handler.onMessageCallback((sess, fixMessage) ->
        {
            final int seqNum = fixMessage.messageSequenceNumber();
            if (seqNum >= 2 && seqNum <= 4)
            {
                reproPositions[seqNum - 2] = reportFactory.trySendReport(sess, Side.SELL);
            }
        });

        final Reply<?> startReply = engine.startReproduction();

        final Session session = acquireSession(CURRENT_SEQUENCE, 1);
        assertEquals(1, session.id());
        final CompositeKey compositeKey = session.compositeKey();
        assertEquals(INITIATOR_ID, compositeKey.remoteCompId());
        assertEquals(ACCEPTOR_ID, compositeKey.localCompId());

        System.out.println("ACQUIRED SESSION");

        testSystem.await("Haven't received messages", () ->
        {
            final List<FixMessage> messages = otfAcceptor.messages();
            messages.removeIf(msg -> !NEW_ORDER_SINGLE_MESSAGE_AS_STR.equals(msg.msgType()));
            return messages.size() >= originalReceivedMessages.size();
        });

        System.out.println("Received messages");
        assertEquals(originalReceivedMessages, otfAcceptor.messages());

        final List<String> reproSentMessages = messageStash.messages();
        testSystem.await("Failed to receive messages", () -> reproSentMessages.size() >= MESSAGES_SENT);

        assertEquals(stripTimesAndChecksums(sentMessages), stripTimesAndChecksums(reproSentMessages));
        assertArrayEquals(sentPositions, reproPositions);

        testSystem.awaitCompletedReply(startReply);
    }

    private List<String> stripTimesAndChecksums(final List<String> messages)
    {
        return messages.stream()
            .map(msg -> msg.replaceAll("\001(52|10)=[^\001]+", ""))
            .collect(Collectors.toList());
    }

    private void createScenario(
        final ReportFactory reportFactory,
        final List<FixMessage> originalReceivedMessages,
        final long[] sentPositions,
        final List<String> sentMessages) throws IOException
    {
        try
        {
            setup(false, true);
            setupLibrary();

            try (FixConnection connection = FixConnection.initiate(port))
            {
                logon(connection);
                sentMessages.add(connection.lastMessageAsString());

                final Session session = acquireSession();

                for (int i = 0; i < MESSAGES_SENT; i++)
                {
                    OrderFactory.sendOrder(connection);
                    originalReceivedMessages.add(
                        testSystem.awaitMessageOf(otfAcceptor, NEW_ORDER_SINGLE_MESSAGE_AS_STR));
                    sentPositions[i] = reportFactory.sendReport(testSystem, session, Side.SELL);
                    otfAcceptor.messages().clear();
                }

                connection.readExecutionReport(2);
                sentMessages.add(connection.lastMessageAsString());
                connection.readExecutionReport(3);
                sentMessages.add(connection.lastMessageAsString());
                connection.readExecutionReport(4);
                sentMessages.add(connection.lastMessageAsString());

                testSystem.awaitSend(session::startLogout);
                connection.readLogout();
                sentMessages.add(connection.lastMessageAsString());
                connection.logout();
                assertSessionDisconnected(testSystem, session);
            }

            libraryId = library.libraryId();
        }
        finally
        {
            teardownArtio();
            closeMediaDriver(mediaDriver);
        }
    }
}
