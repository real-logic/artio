/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.stress;

import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.client.TestReqIdFinder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.Random;

import static uk.co.real_logic.fix_gateway.stress.StressConfiguration.MAX_LENGTH;
import static uk.co.real_logic.fix_gateway.stress.StressConfiguration.MESSAGES_EXCHANGED;
import static uk.co.real_logic.fix_gateway.stress.StressConfiguration.MIN_LENGTH;

final class StressUtil
{
    static void constructMessagePool(final String[] pool, final Random random)
    {
        final byte[] messageContent = new byte[MAX_LENGTH + 1];
        for (int i = 0; i < messageContent.length; i++)
        {
            messageContent[i] = 'X';
        }

        for (int i = 0; i < pool.length; i++)
        {
            final int messageLength = MIN_LENGTH + random.nextInt(MAX_LENGTH - MIN_LENGTH + 1);

            pool[i] = String.format("TestReqId-%d-%s", i, new String(messageContent, 0, messageLength));
        }
    }


    static void exchangeMessages(
        final FixLibrary library,
        final Session session,
        final IdleStrategy idleStrategy,
        final TestReqIdFinder testReqIdFinder,
        final String[] messagePool,
        final Random random)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();

        for (int j = 0; j < MESSAGES_EXCHANGED; j++)
        {
            System.out.format("\rMessage %d", j);

            final String msg = messagePool[random.nextInt(messagePool.length)];
            testRequest.testReqID(msg);

            while (session.send(testRequest) < 0)
            {
                idleStrategy.idle(library.poll(1));
            }

            while (!msg.equals(testReqIdFinder.testReqId()))
            {
                idleStrategy.idle(library.poll(1));
            }

            if (StressConfiguration.PRINT_EXCHANGE)
            {
                System.out.println("Success, received reply!");
                System.out.println(testReqIdFinder.testReqId());
            }
        }
        System.out.format("\r");
    }
}
