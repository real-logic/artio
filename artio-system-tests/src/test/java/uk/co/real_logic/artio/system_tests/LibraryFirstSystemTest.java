/*
 * Copyright 2015-2024 Real Logic Limited.
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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.library.FixLibrary;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class LibraryFirstSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private final ExecutorService threadPool = Executors.newFixedThreadPool(2);

    @Before
    public void launch() throws ExecutionException, InterruptedException
    {
        mediaDriver = launchMediaDriver();
        delete(ACCEPTOR_LOGS);

        final Future<FixLibrary> acceptingLibraryFuture = newAcceptingLibrary();
        final Future<FixLibrary> initiatingLibraryFuture = newInitiatingLibrary();

        waitLessThanReplyTimeout();

        launchAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        acceptingLibrary = acceptingLibraryFuture.get();
        initiatingLibrary = initiatingLibraryFuture.get();
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void engineAndLibraryPairsShouldBeRestartableOutOfOrder()
        throws ExecutionException, InterruptedException
    {
        messagesCanBeExchanged();

        testSystem.close(acceptingLibrary);
        acceptingEngine.close();
        clearMessages();

        final Future<FixLibrary> acceptingLibraryFuture = newAcceptingLibrary();

        waitLessThanReplyTimeout();

        launchAcceptingEngine();
        acceptingLibrary = testSystem.add(acceptingLibraryFuture.get());

        wireSessions();
        messagesCanBeExchanged();
    }

    private void waitLessThanReplyTimeout() throws InterruptedException
    {
        Thread.sleep(TEST_REPLY_TIMEOUT_IN_MS / 4);
    }

    private Future<FixLibrary> newInitiatingLibrary()
    {
        return threadPool.submit(() -> SystemTestUtil.newInitiatingLibrary(
            libraryAeronPort, initiatingHandler, nanoClock));
    }

    private Future<FixLibrary> newAcceptingLibrary()
    {
        return threadPool.submit(() -> SystemTestUtil.newAcceptingLibrary(acceptingHandler, nanoClock));
    }
}
