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
package uk.co.real_logic.fix_gateway.system_tests;

import io.aeron.driver.MediaDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.engine.FixEngine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.agrona.CloseHelper.close;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class MessageBasedSystemTest
{
    private int port = unusedPort();

    private MediaDriver mediaDriver;
    private FixEngine engine;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        engine = FixEngine.launch(acceptingConfig(
            port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID));
    }

    @Test
    public void shouldComplyWIthLogonBasedSequenceNumberReset() throws IOException
    {
        // Trying to reproduce
        // > [8=FIX.4.4|9=0079|35=A|49=BAR|56=FOO|34=1|52=20160825-10:25:03.931|98=0|108=30|141=Y|10=018]
        // < [8=FIX.4.4|9=0079|35=A|49=FOO|56=BAR|34=1|52=20160825-10:24:57.920|98=0|108=30|141=N|10=013]
        // < [8=FIX.4.4|9=0070|35=2|49=FOO|56=BAR|34=3|52=20160825-10:25:27.766|7=1|16=0|10=061]

        logonThenLogout();

        logonThenLogout();
    }

    private void logonThenLogout() throws IOException
    {
        final FixConnection connection = new FixConnection(port);

        final long theFuture = TimeUnit.SECONDS.toMillis(6);
        connection.logon(System.currentTimeMillis() + theFuture);

        connection.readMessage("A");

        connection.logout();

        connection.readMessage("5");

        connection.close();
    }

    @After
    public void tearDown()
    {
        close(engine);
        close(mediaDriver);
        cleanupDirectory(mediaDriver);
    }
}
