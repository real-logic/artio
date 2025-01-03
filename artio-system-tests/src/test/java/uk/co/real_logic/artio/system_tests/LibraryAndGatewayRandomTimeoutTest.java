/*
 * Copyright 2015-2025 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.MonitoringAgentFactory;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.io.File;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.Reply.State.ERRORED;
import static uk.co.real_logic.artio.Reply.State.TIMED_OUT;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class LibraryAndGatewayRandomTimeoutTest
{
    private final EpochNanoClock nanoClock = new OffsetEpochNanoClock();
    private final int aeronPort = unusedPort();
    private final int port = unusedPort();
    private ArchivingMediaDriver mediaDriver;
    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private final FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler initiatingSessionHandler = new FakeHandler(initiatingOtfAcceptor);

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();
    }

    @After
    public void close()
    {
        closeAll(initiatingLibrary, initiatingEngine);
        cleanupMediaDriver(mediaDriver);
    }

    @Test
    public void libraryShouldRefuseConnectionWhenThereIsNoAcceptor()
    {
        launchEngine();
        launchLibrary();

        assertTrue("Library not connected", initiatingLibrary.isConnected());

        initiateResultsInError();
    }

    @Test
    public void libraryShouldRefuseConnectionWhenEngineClosed()
    {
        launchEngine();
        launchLibrary();
        initiatingEngine.close();

        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
        awaitLibraryReply(initiatingLibrary, reply);

        assertEquals(TIMED_OUT, reply.state());
    }

    private void initiateResultsInError()
    {
        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
        awaitLibraryReply(initiatingLibrary, reply);

        assertEquals(ERRORED, reply.state());
        assertThat(reply.error(), instanceOf(FixGatewayException.class));
    }

    private void launchLibrary()
    {
        initiatingLibrary = connect(
            new LibraryConfiguration()
                .sessionAcquireHandler(initiatingSessionHandler)
                .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + aeronPort))
                .monitoringFile(IoUtil.tmpDirName() + "fix-client" + File.separator + "libraryCounters"));
    }

    private void launchEngine()
    {
        final EngineConfiguration initiatingConfig = initiatingConfig(aeronPort, nanoClock);
        initiatingConfig.monitoringAgentFactory(MonitoringAgentFactory.none());
        initiatingConfig.deleteLogFileDirOnStart(true);
        initiatingEngine = FixEngine.launch(initiatingConfig);
    }
}
