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
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.FixGatewayException;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.Reply.State.ERRORED;
import static uk.co.real_logic.fix_gateway.Reply.State.TIMED_OUT;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class LibraryAndGatewayRandomTimeoutTest
{
    private int aeronPort = unusedPort();
    private int port = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler initiatingSessionHandler = new FakeHandler(initiatingOtfAcceptor);

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();
    }

    @Test
    public void libraryShouldRefuseConnectionWhenTheresNoAcceptor()
    {
        launchEngine();
        launchLibrary();

        assertTrue("Library not connected", initiatingLibrary.isConnected());

        initiateResultsInError();
    }

    @Test(expected = IllegalStateException.class)
    public void libraryShouldRefuseConnectionWhenEngineNotStarted()
    {
        launchLibrary();
    }

    @Test
    public void libraryShouldRefuseConnectionWhenEngineClosed()
    {
        launchEngine();
        launchLibrary();
        initiatingEngine.close();

        final Reply<Session> reply = initiateAndAwait(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
        assertEquals(TIMED_OUT, reply.state());
    }

    private void initiateResultsInError()
    {
        final Reply<Session> reply = initiateAndAwait(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
        assertEquals(ERRORED, reply.state());
        assertThat(reply.error(), instanceOf(FixGatewayException.class));
    }

    private void launchLibrary()
    {
        initiatingLibrary = FixLibrary.connect(
            new LibraryConfiguration()
                .sessionAcquireHandler(initiatingSessionHandler)
                .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + aeronPort))
                .monitoringFile(IoUtil.tmpDirName() + "fix-client" + File.separator + "libraryCounters"));
    }

    private void launchEngine()
    {
        initiatingEngine = launchInitiatingGateway(aeronPort);
    }

    @After
    public void close() throws Exception
    {
        CloseHelper.quietClose(initiatingLibrary);
        CloseHelper.quietClose(initiatingEngine);
        CloseHelper.quietClose(mediaDriver);
        cleanupDirectory(mediaDriver);
    }
}
