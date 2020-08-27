/*
 * Copyright 2019 Adaptive Financial Consulting Ltd., Monotonic Ltd.
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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import uk.co.real_logic.artio.OrdType;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.builder.NewOrderSingleEncoder;
import uk.co.real_logic.artio.decoder.NewOrderSingleDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Arrays;
import java.util.Collection;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.Constants.EXECUTION_REPORT_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.alwaysPersistent;

@RunWith(Parameterized.class)
public class PersistentSequenceNumberResendRequestSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final boolean PRINT_ERROR_MESSAGES = true;

    {
        acceptingHandler = new FakeHandler(acceptingOtfAcceptor)
        {
            private final NewOrderSingleDecoder newOrderSingle = new NewOrderSingleDecoder();
            private final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer();
            private final ReportFactory reportFactory = new ReportFactory();

            public Action onMessage(
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final int libraryId,
                final Session session,
                final int sequenceIndex,
                final long messageType,
                final long timestampInNs,
                final long position,
                final OnMessageInfo messageInfo)
            {
                asciiBuffer.wrap(buffer, offset, length);

                if (messageType == NewOrderSingleDecoder.MESSAGE_TYPE)
                {
                    newOrderSingle.decode(asciiBuffer, 0, length);

                    final Action action = reportFactory.sendReport(session, newOrderSingle.sideAsEnum());
                    if (action == ABORT)
                    {
                        return action;
                    }
                }

                return super.onMessage(
                    buffer, offset, length, libraryId, session, sequenceIndex, messageType, timestampInNs, position,
                    messageInfo);
            }
        };
    }

    private final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();
    private final DecimalFloat price = new DecimalFloat(100);
    private final DecimalFloat orderQty = new DecimalFloat(2);
    private final UtcTimestampEncoder transactTime = new UtcTimestampEncoder();
    private final boolean shutdownCleanly;

    @Parameterized.Parameters(name = "shutdownCleanly={0}")
    public static Collection<Object[]> data()
    {
        if (SystemUtil.osName().startsWith("win"))
        {
            return Arrays.asList(new Object[][]{
                {true}
            });
        }
        else
        {
            return Arrays.asList(new Object[][]{
                {true}
            });
        }
    }

    @Before
    public void setUp()
    {
        deleteLogs();
    }

    public PersistentSequenceNumberResendRequestSystemTest(final boolean shutdownCleanly)
    {
        this.shutdownCleanly = shutdownCleanly;
    }

    // TODO: parameters
    // business vs session messages

    @Test
    public void shouldReplayMessageBeforeARestart()
    {
        mediaDriver = TestFixtures.launchMediaDriver();

        // 1. setup a session
        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER);

        // 2. exchange some messages
        sendOrder();

        final FixMessage executionReport =
            testSystem.awaitMessageOf(initiatingOtfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
        final int resendSeqNum = executionReport.messageSequenceNumber();

        assertInitiatingSequenceIndexIs(0);
        if (shutdownCleanly)
        {
            initiatingSession.startLogout();
            assertSessionsDisconnected();

            close();
        }
        else
        {
            CloseHelper.close(initiatingLibrary);
            CloseHelper.close(acceptingLibrary);
            CloseHelper.close(initiatingEngine);
            CloseHelper.close(acceptingEngine);
        }

        clearMessages();
        if (shutdownCleanly)
        {
            launchMediaDriverWithDirs();
        }

        // 4. login with low received sequence number in order to force a resend request from the server.
        launch(1);

        // 5. validate resent message
        final FixMessage resentExecutionReport =
            testSystem.awaitMessageOf(initiatingOtfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
        assertEquals(resendSeqNum, resentExecutionReport.messageSequenceNumber());
        assertEquals("Y", resentExecutionReport.possDup());

        sendResendRequest(1, 3, initiatingOtfAcceptor, initiatingSession);
        sendResendRequest(1, 3, initiatingOtfAcceptor, initiatingSession);

        assertEventuallyTrue(() -> "Failed to receive all the resends: " + initiatingOtfAcceptor.messages(),
            () ->
            {
                testSystem.poll();

                assertEquals(2, initiatingOtfAcceptor
                    .receivedReplayGapFill(1, 2)
                    .count());

                assertEquals(2, initiatingOtfAcceptor
                    .receivedReplay(EXECUTION_REPORT_MESSAGE_AS_STR, resendSeqNum)
                    .count());

                assertEquals(2, initiatingOtfAcceptor
                    .receivedReplayGapFill(3, 4)
                    .count());
            }, 5000);
    }

    private void launch(final int initiatorInitialReceivedSequenceNumber)
    {
        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.sessionPersistenceStrategy(alwaysPersistent());
        acceptingConfig.printStartupWarnings(PRINT_ERROR_MESSAGES);
        acceptingConfig.printErrorMessages(PRINT_ERROR_MESSAGES);
        acceptingConfig.gracefulShutdown(shutdownCleanly);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        initiatingConfig.printStartupWarnings(PRINT_ERROR_MESSAGES);
        initiatingConfig.printErrorMessages(PRINT_ERROR_MESSAGES);
        initiatingConfig.gracefulShutdown(shutdownCleanly);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibraryConfig.gracefulShutdown(shutdownCleanly);
        testSystem = new TestSystem();
        acceptingLibrary = testSystem.connect(acceptingLibraryConfig);

        final LibraryConfiguration initiatingLibraryConfig =
            initiatingLibraryConfig(libraryAeronPort, initiatingHandler);
        initiatingLibraryConfig.gracefulShutdown(shutdownCleanly);
        initiatingLibrary = testSystem.connect(initiatingLibraryConfig);

        final Reply<Session> reply = connectPersistentSessions(
            AUTOMATIC_INITIAL_SEQUENCE_NUMBER, initiatorInitialReceivedSequenceNumber, false);
        assertEquals("Reply failed: " + reply, Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();
        acquireAcceptingSession();
    }

    private void sendOrder()
    {
        final int transactTimeLength = transactTime.encode(System.currentTimeMillis());

        newOrderSingle
            .clOrdID("A")
            .side(Side.BUY)
            .transactTime(transactTime.buffer(), transactTimeLength)
            .ordType(OrdType.MARKET)
            .price(price);

        newOrderSingle.instrument().symbol("MSFT");
        newOrderSingle.orderQtyData().orderQty(orderQty);

        final long position = initiatingSession.trySend(newOrderSingle);
        assertThat(position, Matchers.greaterThan(0L));
    }
}
