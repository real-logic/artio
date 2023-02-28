/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.stress;

import org.agrona.IoUtil;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.client.TestReqIdFinder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static uk.co.real_logic.artio.CommonConfiguration.optimalTmpDirName;
import static uk.co.real_logic.artio.stress.StressConfiguration.*;

final class StressUtil
{
    static String[] constructMessagePool(final String prefix, final Random random)
    {
        final String[] pool = new String[StressConfiguration.MESSAGE_POOL];

        final byte[] messageContent = new byte[MAX_LENGTH + 1];
        Arrays.fill(messageContent, (byte)'X');

        for (int i = 0; i < pool.length; i++)
        {
            final int messageLength = MIN_LENGTH + random.nextInt(MAX_LENGTH - MIN_LENGTH + 1);

            pool[i] = String.format("%sTestReqId-%d-%s", prefix, i, new String(messageContent, 0, messageLength));
        }

        return pool;
    }


    static void exchangeMessages(
        final FixLibrary library,
        final Session session,
        final IdleStrategy idleStrategy,
        final TestReqIdFinder testReqIdFinder,
        final String[] messagePool,
        final Random random,
        final String senderCompId)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();

        for (int j = 0; j < MESSAGES_EXCHANGED; j++)
        {
            if (!StressConfiguration.PRINT_EXCHANGE)
            {
                System.out.format("\rMessage %d", j);
            }

            final String msg = messagePool[random.nextInt(messagePool.length)];
            testRequest.testReqID(msg);

            while (session.trySend(testRequest) < 0)
            {
                idleStrategy.idle(library.poll(1));
            }

            for (long fails = 0; !msg.equals(testReqIdFinder.testReqId()); fails++)
            {
                if (StressConfiguration.printFailedSpints(fails))
                {
                    System.out.println(senderCompId + " Has repeatedly failed for " + msg);
                    fails = 0;
                }

                idleStrategy.idle(library.poll(1));
            }

            if (StressConfiguration.PRINT_EXCHANGE)
            {
                System.out.println(senderCompId + " Success, received reply! " + msg);
            }
        }

        if (!StressConfiguration.PRINT_EXCHANGE)
        {
            System.out.format("\r");
        }
    }

    static void cleanupOldLogFileDir(final EngineConfiguration configuration)
    {
        IoUtil.delete(new File(configuration.logFileDir()), true);

        final File tmpDir = new File(optimalTmpDirName());
        for (final File file : tmpDir.listFiles(file -> file.getName().contains("fix-library-")))
        {
            IoUtil.delete(file, false);
        }
    }

    static void awaitKeyPress()
    {
        System.out.println("Press any key to exit");
        try
        {
            System.in.read();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

}
