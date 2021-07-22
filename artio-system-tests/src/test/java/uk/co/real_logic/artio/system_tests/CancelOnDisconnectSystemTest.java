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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.CancelOnDisconnectType;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.builder.AbstractLogonEncoder;
import uk.co.real_logic.artio.builder.AbstractLogoutEncoder;
import uk.co.real_logic.artio.builder.LogonEncoder;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.CancelOnDisconnectOption;
import uk.co.real_logic.artio.session.CancelOnDisconnectTimeoutHandler;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.SessionCustomisationStrategy;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static uk.co.real_logic.artio.CancelOnDisconnectType.*;
import static uk.co.real_logic.artio.CommonConfiguration.RUNNING_ON_WINDOWS;
import static uk.co.real_logic.artio.Constants.LOGON_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class CancelOnDisconnectSystemTest extends AbstractGatewayToGatewaySystemTest
{
    public static final int COD_TEST_TIMEOUT_IN_MS = 500;
    public static final int LONG_COD_TEST_TIMEOUT_IN_MS = RUNNING_ON_WINDOWS ? 3_000 : COD_TEST_TIMEOUT_IN_MS;

    private final FakeTimeoutHandler timeoutHandler = new FakeTimeoutHandler();

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .deleteLogFileDirOnStart(true)
            .cancelOnDisconnectTimeoutHandler(timeoutHandler));
        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        acceptingLibrary = connect(acceptingLibraryConfig(acceptingHandler, nanoClock));
    }

    private void setup(final int cancelOnDisconnectType, final int cODTimeoutWindow)
    {
        final CancelOnDisconnectSessionCustomisationStrategy customisationStrategy =
            new CancelOnDisconnectSessionCustomisationStrategy(cancelOnDisconnectType, cODTimeoutWindow);
        setup(customisationStrategy);
    }

    private void setup(final SessionCustomisationStrategy customisationStrategy)
    {
        final LibraryConfiguration config = initiatingLibraryConfig(libraryAeronPort, initiatingHandler, nanoClock);
        config.sessionCustomisationStrategy(customisationStrategy);
        initiatingLibrary = connect(config);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTriggerCancelOnDisconnectTimeoutForLogout()
    {
        setup(CANCEL_ON_LOGOUT_ONLY.representation(), COD_TEST_TIMEOUT_IN_MS);

        logoutSession(initiatingSession);

        assertTriggersCancelOnDisconnect(CANCEL_ON_LOGOUT_ONLY);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTriggerCancelOnDisconnectTimeoutForDisconnect()
    {
        setup(CANCEL_ON_DISCONNECT_ONLY.representation(), COD_TEST_TIMEOUT_IN_MS);

        testSystem.awaitRequestDisconnect(initiatingSession);

        assertTriggersCancelOnDisconnect(CANCEL_ON_DISCONNECT_ONLY);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTriggerCancelOnDisconnectTimeoutForLogoutLibrary()
    {
        setup(CANCEL_ON_LOGOUT_ONLY.representation(), COD_TEST_TIMEOUT_IN_MS);

        acquireAcceptingSession();

        logoutSession(initiatingSession);

        assertTriggersCancelOnDisconnect(CANCEL_ON_LOGOUT_ONLY);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTriggerCancelOnDisconnectTimeoutForDisconnectLibrary()
    {
        setup(CANCEL_ON_DISCONNECT_ONLY.representation(), COD_TEST_TIMEOUT_IN_MS);

        testSystem.awaitRequestDisconnect(initiatingSession);

        assertTriggersCancelOnDisconnect(CANCEL_ON_DISCONNECT_ONLY);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotTriggerCancelOnDisconnectTimeoutWhenConfiguredNotTo()
    {
        setup(DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT.representation(), 0);

        testSystem.awaitRequestDisconnect(initiatingSession);
        assertDisconnectWithHandlerNotInvoked();

        assertInitiatorCodState(DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT, 0, 0);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotTriggerCancelOnDisconnectTimeoutWithNoLogonOptions()
    {
        setup(SessionCustomisationStrategy.none());

        testSystem.awaitRequestDisconnect(initiatingSession);
        assertDisconnectWithHandlerNotInvoked();

        assertInitiatorCodState(DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT, 0, 0);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCorrectTimeoutsOverLimit()
    {
        setup(CANCEL_ON_DISCONNECT_OR_LOGOUT.representation(), 100_000_000);

        acquireAcceptingSession();

        final long maxTimeoutInNs = 60_000_000_000L;
        assertEquals(maxTimeoutInNs, acceptingSession.cancelOnDisconnectTimeoutWindowInNs());
        assertEquals(maxTimeoutInNs, initiatingSession.cancelOnDisconnectTimeoutWindowInNs());
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotTriggerCancelOnDisconnectTimeoutIfReconnectOccurs()
    {
        setup(CANCEL_ON_DISCONNECT_OR_LOGOUT.representation(), LONG_COD_TEST_TIMEOUT_IN_MS);

        testSystem.awaitRequestDisconnect(initiatingSession);

        assertSessionDisconnected(initiatingSession);

        connectSessions();

        assertHandlerNotInvoked(LONG_COD_TEST_TIMEOUT_IN_MS);
    }

    private void assertDisconnectWithHandlerNotInvoked()
    {
        assertSessionDisconnected(initiatingSession);

        assertHandlerNotInvoked(COD_TEST_TIMEOUT_IN_MS);
    }

    private void assertHandlerNotInvoked(final int codTestTimeoutInMs)
    {
        testSystem.awaitBlocking(() ->
        {
            try
            {
                Thread.sleep(codTestTimeoutInMs);
            }
            catch (final InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        assertNull(timeoutHandler.result);
        assertEquals(0, timeoutHandler.invokeCount());
    }

    private void assertTriggersCancelOnDisconnect(final CancelOnDisconnectType type)
    {
        final long codTimeoutInNs = MILLISECONDS.toNanos(COD_TEST_TIMEOUT_IN_MS);

        final long logoutTimeInNs = nanoClock.nanoTime();
        assertSessionDisconnected(initiatingSession);

        testSystem.await("timeout not triggered", () -> timeoutHandler.result() != null);

        final SessionInfo onlySession = acceptingEngine.allSessions().get(0);
        final TimeoutResult result = timeoutHandler.result();
        assertEquals(onlySession.sessionId(), result.surrogateId);
        assertEquals(onlySession.sessionKey(), result.compositeId);
        final long timeoutTakenInNs = result.timeInNs - logoutTimeInNs;
        assertThat(timeoutTakenInNs, greaterThanOrEqualTo(codTimeoutInNs));
        assertEquals(1, timeoutHandler.invokeCount());

        assertInitiatorCodState(type, codTimeoutInNs, COD_TEST_TIMEOUT_IN_MS);
    }

    private void assertInitiatorCodState(
        final CancelOnDisconnectType type, final long codTimeoutInNs, final int codTestTimeoutInMs)
    {
        final FixMessage echodLogon = initiatingOtfAcceptor.messages().get(0);
        assertEquals(LOGON_MESSAGE_AS_STR, echodLogon.msgType());
        assertEquals(echodLogon.toString(), type.representation(), echodLogon.cancelOnDisconnectType());
        assertEquals(echodLogon.toString(), codTestTimeoutInMs, echodLogon.getInt(Constants.C_O_D_TIMEOUT_WINDOW));

        final CancelOnDisconnectOption option = CancelOnDisconnectOption.get(type.representation());
        assertEquals(option, initiatingSession.cancelOnDisconnectOption());
        assertEquals(codTimeoutInNs, initiatingSession.cancelOnDisconnectTimeoutWindowInNs());
    }

    static class CancelOnDisconnectSessionCustomisationStrategy implements SessionCustomisationStrategy
    {
        private final int cancelOnDisconnectType;
        private final int cODTimeoutWindow;

        CancelOnDisconnectSessionCustomisationStrategy(
            final int cancelOnDisconnectType, final int cODTimeoutWindow)
        {
            this.cancelOnDisconnectType = cancelOnDisconnectType;
            this.cODTimeoutWindow = cODTimeoutWindow;
        }

        public void configureLogon(final AbstractLogonEncoder abstractLogon, final long sessionId)
        {
            final LogonEncoder logon = (LogonEncoder)abstractLogon;
            logon.cancelOnDisconnectType(cancelOnDisconnectType);
            logon.cODTimeoutWindow(cODTimeoutWindow);
        }

        public void configureLogout(final AbstractLogoutEncoder logout, final long sessionId)
        {
        }
    }

    class FakeTimeoutHandler implements CancelOnDisconnectTimeoutHandler
    {
        private final AtomicInteger invokeCount = new AtomicInteger(0);
        private volatile TimeoutResult result;

        public void onCancelOnDisconnectTimeout(final long sessionId, final CompositeKey fixSessionKey)
        {
            this.result = new TimeoutResult(sessionId, fixSessionKey);
            invokeCount.incrementAndGet();
        }

        public TimeoutResult result()
        {
            return result;
        }

        public int invokeCount()
        {
            return invokeCount.get();
        }
    }

    final class TimeoutResult
    {
        private final long surrogateId;
        private final CompositeKey compositeId;
        private final long timeInNs;

        private TimeoutResult(final long surrogateId, final CompositeKey compositeId)
        {
            this.surrogateId = surrogateId;
            this.compositeId = compositeId;
            timeInNs = nanoClock.nanoTime();
        }
    }

}
