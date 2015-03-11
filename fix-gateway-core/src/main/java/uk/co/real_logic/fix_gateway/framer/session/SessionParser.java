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
package uk.co.real_logic.fix_gateway.framer.session;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

public class SessionParser
{
    private final AsciiFlyweight string = new AsciiFlyweight();
    private final LogonDecoder logon = new LogonDecoder();
    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final LogoutDecoder logout = new LogoutDecoder();
    private final HeartbeatDecoder heartbeat = new HeartbeatDecoder();
    private final RejectDecoder reject = new RejectDecoder();

    private final Session session;
    private long sessionId;

    public SessionParser(final Session session)
    {
        this.session = session;
    }

    public long onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long connectionId,
        final int messageType)
    {
        string.wrap(buffer);

        // TODO: headers
        // TODO: session id lookup
        final long sessionId = connectionId;
        final int msgSeqNo = 0;

        switch (messageType)
        {
            case LogonDecoder.MESSAGE_TYPE:
                logon.decode(string, offset, length);
                session.onLogon(logon.heartBtInt(), msgSeqNo, sessionId);
                break;

            case ResendRequestDecoder.MESSAGE_TYPE:
                resendRequest.decode(string, offset, length);
                session.onResendRequest(resendRequest.beginSeqNo(), resendRequest.endSeqNo());
                break;

            case LogoutDecoder.MESSAGE_TYPE:
                logout.decode(string, offset, length);
                session.onLogout(msgSeqNo, sessionId);
                break;

            case HeartbeatDecoder.MESSAGE_TYPE:
                heartbeat.decode(string, offset, length);

                break;
        }

        session.onMessage(msgSeqNo);
        return connectionId;
    }

}
