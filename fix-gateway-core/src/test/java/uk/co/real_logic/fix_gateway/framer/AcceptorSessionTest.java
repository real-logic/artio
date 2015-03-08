/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.framer;

import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.fix_gateway.framer.SessionState.*;

public class AcceptorSessionTest
{
    private static final long CONNECTION_ID = 3L;
    private static final long SESSION_ID = 2L;
    private static final long HEARTBEAT_INTERVAL = 1L;

    private SessionProxy mockProxy = mock(SessionProxy.class);
    private MilliClock mockClock = mock(MilliClock.class);

    private AcceptorSession session = new AcceptorSession(HEARTBEAT_INTERVAL, CONNECTION_ID, mockClock, mockProxy);

    @Test
    public void shouldInitiallyBeConnected()
    {
        assertEquals(CONNECTED, session.state());
    }

    @Test
    public void shouldBeActivatedBySuccessfulLogin()
    {
        onLogin(1);

        verify(mockProxy).logon(HEARTBEAT_INTERVAL, 2, SESSION_ID);
        assertState(ACTIVE);
    }

    @Test
    public void shouldRequestResendIfHighSeqNoLogon()
    {
        onLogin(3);

        verify(mockProxy).resendRequest(1, 2);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfLowSeqNoLogon()
    {
        session.lastMsgSeqNum(2);

        onLogin(1);
        verify(mockProxy).disconnect(CONNECTION_ID);
        assertState(DISCONNECTED);
    }

    @Ignore
    @Test
    public void shouldDisconnectOnValidLogout()
    {

    }

    @Ignore
    @Test
    public void shouldRequestResendIfHighSeqNoLogout()
    {

    }

    @Ignore
    @Test
    public void shouldDisconnectIfFirstMessageNotALogon()
    {

    }

    @Ignore
    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {

    }

    /*Receive Sequence Reset (Gap Fill) message with NewSeqNo > MsgSeqNum

    MsgSeqNum > than expect sequence number

    MsgSeqNum = to expected sequence number

    MsgSeqNum < than expected sequence number
    1. If MsgSeqNum > expected
    Issue Resend Request to fill gap between expected MsgSeqNum & MsgSeqNum.

    2. If MsgSeqNum < expected sequence number & PossDupFlag = “Y”
    Ignore message

    3. If MsgSeqNum < expected sequence number & without PossDupFlag = “Y”
    Disconnect without sending a message
    Generate an "error" condition in test output

    4. if MsgSeqNum = expected sequence number.
    Set next expected sequence number = NewSeqNo

    Receive Resend Request message
    Mandatory
    Valid Resend Request
    Respond with application level messages and SequenceReset-Gap Fill for admin messages in requested range according to "Message Recovery" rules.

    Receive Sequence Reset (Reset)
    Mandatory
    a. Receive Sequence Reset (reset) message with NewSeqNo > than expected sequence number
    1) Accept the Sequence Reset (Reset) message without regards its MsgSeqNum
    2) Set expected sequence number equal to NewSeqNo

    b. Receive Sequence Reset (reset) message with NewSeqNo = to expected sequence number
    1) Accept the Sequence Reset (Reset) message without regards its MsgSeqNum
    2) Generate a "warning" condition in test output.

    c. Receive Sequence Reset (reset) message with NewSeqNo < than expected sequence number
    1) Accept the Sequence Reset (Reset) message without regards its MsgSeqNum
    2) Send Reject (session-level) message referencing invalid MsgType (>= FIX 4.2: SessionRejectReason = "Value is incorrect (out of range) for this tag")
        3) Do NOT Increment inbound MsgSeqNum
    4) Generate an "error" condition in test output
    5) Do NOT lower expected sequence number.
    */



    private void assertState(final SessionState state)
    {
        assertEquals(state, session.state());
    }

    private void onLogin(final int msgSeqNum)
    {
        session.onLogin(HEARTBEAT_INTERVAL, msgSeqNum, SESSION_ID);
    }
}
