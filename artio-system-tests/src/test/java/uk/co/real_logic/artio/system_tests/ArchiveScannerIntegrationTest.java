/*
 * Copyright 2015-2021 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.CloseHelper;
import org.agrona.collections.IntHashSet;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.library.LibraryConfiguration;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static uk.co.real_logic.artio.TestFixtures.largeTestReqId;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ArchiveScannerIntegrationTest extends AbstractGatewayToGatewaySystemTest
{
    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibraryConfig.libraryConnectHandler(fakeConnectHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhilstGatewayRunningOneStream()
    {
        setupAndExchangeMessages();

        assertOutboundArchiveContainsMessages("hi");
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhilstGatewayRunningBothStreams()
    {
        setupAndExchangeMessages();

        assertArchiveContainsBothMessages("hi");
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveForLargeMessages()
    {
        acquireAcceptingSession();

        final String testReqID = largeTestReqId();

        sendTestRequest(acceptingSession, testReqID);

        assertReceivedSingleHeartbeat(testSystem, acceptingOtfAcceptor, testReqID);

        assertInitiatingSequenceIndexIs(0);

        assertOutboundArchiveContainsMessages(largeTestReqId());
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhenGatewayStoppedOneStream()
    {
        setupAndExchangeMessages();

        closeLibrariesAndEngines();

        assertOutboundArchiveContainsMessages("hi");
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhenGatewayStoppedBothStreams()
    {
        setupAndExchangeMessages();

        closeLibrariesAndEngines();

        assertArchiveContainsBothMessages("hi");
    }

    private void closeLibrariesAndEngines()
    {
        CloseHelper.close(initiatingLibrary);
        CloseHelper.close(acceptingLibrary);

        CloseHelper.close(initiatingEngine);
        CloseHelper.close(acceptingEngine);
    }

    private void setupAndExchangeMessages()
    {
        messagesCanBeExchanged();

        assertInitiatingSequenceIndexIs(0);
    }

    private void assertOutboundArchiveContainsMessages(final String testReqIdPrefix)
    {
        final EngineConfiguration configuration = acceptingEngine.configuration();
        final List<String> messages = getMessagesFromArchive(
            configuration, configuration.outboundLibraryStream());

        assertThat(messages.toString(), messages, hasItems(
            containsString("35=A\00149=acceptor\00156=initiator\00134=1"),
            containsString("\001112=" + testReqIdPrefix)));
    }

    private void assertArchiveContainsBothMessages(final String testReqIdPrefix)
    {
        final EngineConfiguration configuration = acceptingEngine.configuration();
        final IntHashSet queryStreamIds = new IntHashSet();
        queryStreamIds.add(configuration.outboundLibraryStream());
        queryStreamIds.add(configuration.inboundLibraryStream());
        final List<String> messages = getMessagesFromArchive(configuration, queryStreamIds);

        final List<String> first4Messages = messages.subList(0, 4);
        assertThat(messages.toString(), first4Messages, contains(
            containsString("35=A\00149=initiator\00156=acceptor\00134=1"),
            containsString("35=A\00149=acceptor\00156=initiator\00134=1"),
            containsString("35=1\00149=initiator\00156=acceptor\00134=2"),
            containsString("\001112=" + testReqIdPrefix)));
    }

}
