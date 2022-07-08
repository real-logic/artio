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
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ReproductionSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
    @Test
    public void shouldReproduceMessageExchange() throws IOException
    {
        final ReportFactory reportFactory = new ReportFactory();

        final long startInNs;
        final long endInNs;
        try
        {
            setup(false, true);
            setupLibrary();

            startInNs = nanoClock.nanoTime();
            try (final FixConnection connection = FixConnection.initiate(port))
            {
                logon(connection);

                final Session session = acquireSession();

                for (int i = 0; i < 3; i++)
                {
                    OrderFactory.sendOrder(connection);
                    testSystem.awaitMessageOf(otfAcceptor, Constants.NEW_ORDER_SINGLE_MESSAGE_AS_STR);
                    reportFactory.sendReport(testSystem, session, Side.SELL);
                    otfAcceptor.messages().clear();
                }

                connection.readExecutionReport(2);
                connection.readExecutionReport(3);
                connection.readExecutionReport(4);

                testSystem.awaitSend(session::startLogout);
                connection.readLogout();
                connection.logout();
                assertSessionDisconnected(testSystem, session);
            }

            endInNs = nanoClock.nanoTime();
        }
        finally
        {
            teardownArtio();
            mediaDriver.close();
        }

        // TODO: logon and perform a replay

        setup(false, true, true, InitialAcceptedSessionOwner.ENGINE, false,
            true, startInNs, endInNs, false);
        setupLibrary();

        engine.startReproduction();

        final Session session = acquireSession();
        assertEquals(1, session.id());
        final CompositeKey compositeKey = session.compositeKey();
        assertEquals(INITIATOR_ID, compositeKey.remoteCompId());
        assertEquals(ACCEPTOR_ID, compositeKey.localCompId());

        // TODO: how to sync the library ids?

        /*for (int i = 0; i < 3; i++)
        {
            testSystem.awaitMessageOf(otfAcceptor, Constants.NEW_ORDER_SINGLE_MESSAGE_AS_STR);
            // TODO: check that these are identical to the original orders
            reportFactory.sendReport(testSystem, session, Side.SELL);
            otfAcceptor.messages().clear();
        }*/

        // TODO: check that our callback gets some reports and that they are identical?
        // TODO: send some reports in response
    }

    // TODO: what we if we have a different number of libraries?
    // TODO: maybe log what is changed in terms of library ids?
}
