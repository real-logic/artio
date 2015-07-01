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
package uk.co.real_logic.fix_gateway.session;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.library.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

public class SessionParser
{

    private final AsciiFlyweight string = new AsciiFlyweight();
    private final LogonDecoder logon = new LogonDecoder();
    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final LogoutDecoder logout = new LogoutDecoder();
    private final RejectDecoder reject = new RejectDecoder();
    private final TestRequestDecoder testRequest = new TestRequestDecoder();
    private final HeaderDecoder header = new HeaderDecoder();

    private final Session session;
    private final SessionIdStrategy sessionIdStrategy;
    private final AuthenticationStrategy authenticationStrategy;

    public SessionParser(
        final Session session,
        final SessionIdStrategy sessionIdStrategy,
        final AuthenticationStrategy authenticationStrategy)
    {
        this.session = session;
        this.sessionIdStrategy = sessionIdStrategy;
        this.authenticationStrategy = authenticationStrategy;
    }

    public boolean onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int messageType,
        final long sessionId)
    {
        string.wrap(buffer);

        switch (messageType)
        {
            case LogonDecoder.MESSAGE_TYPE:
                logon.reset();
                logon.decode(string, offset, length);
                if (authenticationStrategy.authenticate(logon))
                {
                    final HeaderDecoder header = logon.header();
                    final Object sessionKey = sessionIdStrategy.onAcceptorLogon(header);

                    if (session.onBeginString(header.beginString(), header.beginStringLength()))
                    {
                        session.onLogon(
                            logon.heartBtInt(),
                            header.msgSeqNum(),
                            sessionId,
                            sessionKey,
                            header.sendingTime(),
                            isPossDup(header));
                    }
                }
                else
                {
                    session.disconnect();
                }
                break;

            case LogoutDecoder.MESSAGE_TYPE:
            {
                logout.reset();
                logout.decode(string, offset, length);
                final HeaderDecoder header = logout.header();
                session.onLogout(header.msgSeqNum(), isPossDup(header));
                break;
            }

            case RejectDecoder.MESSAGE_TYPE:
            {
                reject.reset();
                reject.decode(string, offset, length);
                final HeaderDecoder header = reject.header();
                session.onReject(header.msgSeqNum(), isPossDup(header));
                break;
            }

            case TestRequestDecoder.MESSAGE_TYPE:
            {
                testRequest.reset();
                testRequest.decode(string, offset, length);
                final HeaderDecoder header = testRequest.header();
                final int msgSeqNo = header.msgSeqNum();
                session.onTestRequest(
                    testRequest.testReqID(), testRequest.testReqIDLength(), msgSeqNo, isPossDup(header));
                break;
            }

            default:
            {
                final HeaderDecoder header = this.header;
                header.reset();
                header.decode(string, offset, length);
                session.onMessage(header.msgSeqNum(), isPossDup(header));
                break;
            }
        }

        return session.isConnected();
    }

    private boolean isPossDup(final HeaderDecoder header)
    {
        return (header.hasPossDupFlag() && header.possDupFlag())
            || (header.hasPossResend() && header.possResend());
    }

    public Session session()
    {
        return session;
    }
}
