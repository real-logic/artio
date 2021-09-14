/*
 * Copyright 2021 Monotonic Ltd.
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

import uk.co.real_logic.artio.session.Session;

import java.io.File;

import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

/**
 * Makes a big archive example for {@link ArchiveScannerBenchmark}.
 */
public class ArchiveScannerBenchmarkGenerator extends ArchiveScannerIntegrationTest
{
    static final int SEND_BATCH = 5_000;

    public static void main(final String[] args)
    {
        new ArchiveScannerBenchmarkGenerator().setupBenchmark();
    }

    private void setupBenchmark()
    {
        System.out.println("Running in: " + new File(".").getAbsolutePath());

        launch();

        try
        {
            final int otherSessionCount = 4;
            final int initialSuffix = 2;
            final Session[] sessions = new Session[otherSessionCount];
            for (int i = 0; i < otherSessionCount; i++)
            {
                sessions[i] = completeConnectSessions(initiate(
                    initiatingLibrary, port, INITIATOR_ID + (i + initialSuffix), ACCEPTOR_ID));
            }

            for (int i = 0; i < 900; i++)
            {
                exchangeMessages(SEND_BATCH);

                System.out.println(i);
            }

            final long start = nanoClock.nanoTime();

            for (int i = 0; i < otherSessionCount; i++)
            {
                exchangeMessages(SEND_BATCH, sessions[i]);
            }

            exchangeMessages(SEND_BATCH);

            final long end = nanoClock.nanoTime();

            for (int i = 0; i < 900; i++)
            {
                exchangeMessages(SEND_BATCH);

                System.out.println(i);
            }

            System.out.println("start = " + start);
            System.out.println("end = " + end);

            System.out.println("mediaDriver = " + mediaDriver.mediaDriver().aeronDirectoryName());
            final File archiveDir = mediaDriver.archive().context().archiveDir();
            System.out.println("mediaDriver.archive().context().archiveDir() = " + archiveDir);
            System.out.println("acceptingEngine = " + acceptingEngine.configuration().logFileDir());
            System.out.println("new File(\".\") = " + new File(".").getAbsolutePath());
        }
        finally
        {
            close();
        }
    }

    private void exchangeMessages(final int n)
    {
        exchangeMessages(n, initiatingSession);
    }

    private void exchangeMessages(final int n, final Session session)
    {
        String testReqID = null;
        for (int i = 0; i < n; i++)
        {
            testReqID = testReqId();
            sendTestRequest(testSystem, session, testReqID);
        }
        assertReceivedSingleHeartbeat(testSystem, initiatingOtfAcceptor, testReqID);
        initiatingOtfAcceptor.messages().clear();
    }
}
