/*
 * Copyright 2015-2022 Real Logic Limited.
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
package uk.co.real_logic.artio.session;

import org.junit.Test;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.CommonConfiguration.*;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_REASONABLE_TRANSMISSION_TIME_IN_MS;
import static uk.co.real_logic.artio.messages.SessionState.*;
import static uk.co.real_logic.artio.session.DirectSessionProxy.NO_LAST_MSG_SEQ_NUM_PROCESSED;
import static uk.co.real_logic.artio.session.Session.UNKNOWN_TIME;

public class InitiatorSessionTest extends AbstractSessionTest
{

    protected Session newSession()
    {
        final InitiatorSession session = new InitiatorSession(HEARTBEAT_INTERVAL_IN_S,
            CONNECTION_ID,
            fakeClock,
            nanoClock,
            sessionProxy,
            mock(GatewayPublication.class),
            mockPublication,
            idStrategy,
            SENDING_TIME_WINDOW,
            mockReceivedMsgSeqNo,
            mockSentMsgSeqNo,
            LIBRARY_ID,
            1,
            SEQUENCE_INDEX,
            CONNECTED,
            false,
            DEFAULT_REASONABLE_TRANSMISSION_TIME_IN_MS,
            new MutableAsciiBuffer(new byte[DEFAULT_SESSION_BUFFER_SIZE]),
            false,
            SessionCustomisationStrategy.none(),
            messageInfo,
            fakeEpochFractionClock,
            true,
            DEFAULT_RESEND_REQUEST_CONTROLLER,
            forcedHeartbeatIntervalInS);
        session.fixDictionary(makeDictionary());
        session.sessionProcessHandler(fixSessionOwner);
        return session;
    }

    @Test
    public void shouldInitiallyBeConnected()
    {
        assertEquals(CONNECTED, session().state());
    }

    @Test
    public void shouldActivateUponLogonResponse()
    {
        session().state(SENT_LOGON);

        assertEquals(CONTINUE, onLogon(1));

        assertState(ACTIVE);
        verify(sessionProxy).seqNumResetRequested();
        verifyNoFurtherMessages();
        verifyNotifiesLoginListener();
        assertHasLogonTime();
    }

    @Test
    public void shouldAttemptLogonWhenConnected()
    {
        session().id(SESSION_ID);
        session().poll(0);

        verifyLogon();

        assertEquals(1, session().lastSentMsgSeqNum());
    }

    @Test
    public void shouldAttemptLogonOnlyOnce()
    {
        session().id(SESSION_ID);
        session().poll(0);

        session().poll(10_000_000);

        session().poll(20_000_000);

        verifyLogon();
    }

    @Test
    public void shouldNotifyGatewayWhenLoggedIn()
    {
        session().state(SENT_LOGON);

        assertEquals(CONTINUE, onLogon(1));

        verifyNotifiesLoginListener();
        assertHasLogonTime();
    }

    @Test
    public void shouldNotifyGatewayWhenLoggedInOnce()
    {
        session().state(SENT_LOGON);

        assertEquals(CONTINUE, onLogon(1));

        assertEquals(CONTINUE, onLogon(2));

        verifyNotifiesLoginListener();
        assertHasLogonTime();
    }

    @Test
    public void shouldStartAcceptLogonBasedSequenceNumberResetWhenSequenceNumberIsOne()
    {
        shouldStartAcceptLogonBasedSequenceNumberResetWhenSequenceNumberIsOne(SEQUENCE_INDEX);
    }

    private void verifyLogon()
    {
        verify(sessionProxy, times(1)).sendLogon(
            1, HEARTBEAT_INTERVAL_IN_S, null, null, false, SEQUENCE_INDEX, NO_LAST_MSG_SEQ_NUM_PROCESSED,
            null, MISSING_INT);
    }

    protected void readyForLogon()
    {
        session().state(SENT_LOGON);
    }

    private void assertHasLogonTime()
    {
        assertTrue(session().lastLogonTimeInNs() != UNKNOWN_TIME);
    }
}
