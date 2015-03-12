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

// TODO: optionally validate session on every message?
public class SessionParser
{
    public static final long UNKNOWN_SESSION_ID = -1;

    private final AsciiFlyweight string = new AsciiFlyweight();
    private final LogonDecoder logon = new LogonDecoder();
    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final LogoutDecoder logout = new LogoutDecoder();
    private final RejectDecoder reject = new RejectDecoder();
    private final HeaderDecoder header = new HeaderDecoder();

    private final Session session;
    private final SessionIdStrategy sessionIdStrategy;

    private long sessionId;

    public SessionParser(final Session session, final SessionIdStrategy sessionIdStrategy)
    {
        this.session = session;
        this.sessionIdStrategy = sessionIdStrategy;
    }

    public long onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long connectionId,
        final int messageType)
    {
        string.wrap(buffer);

        int msgSeqNo = 0;

        switch (messageType)
        {
            case LogonDecoder.MESSAGE_TYPE:
                logon.decode(string, offset, length);
                msgSeqNo = logon.header().msgSeqNum();
                sessionId = sessionIdStrategy.identify(header);
                session.onLogon(logon.heartBtInt(), msgSeqNo, sessionId);
                break;

            case ResendRequestDecoder.MESSAGE_TYPE:
                resendRequest.decode(string, offset, length);
                msgSeqNo = resendRequest.header().msgSeqNum();

                session.onResendRequest(resendRequest.beginSeqNo(), resendRequest.endSeqNo());
                break;

            case LogoutDecoder.MESSAGE_TYPE:
                logout.decode(string, offset, length);
                msgSeqNo = logout.header().msgSeqNum();

                session.onLogout(msgSeqNo, sessionId);
                break;

            case RejectDecoder.MESSAGE_TYPE:
                reject.decode(string, offset, length);
                msgSeqNo = reject.header().msgSeqNum();
                session.onReject();
                break;

            default:
                header.decode(string, offset, length);
                msgSeqNo = header.msgSeqNum();
                break;
        }

        session.onMessage(msgSeqNo);
        if (session.isConnected())
        {
            return sessionId;
        }
        else
        {
            return UNKNOWN_SESSION_ID;
        }
    }

}
