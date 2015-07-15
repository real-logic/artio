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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.library.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static uk.co.real_logic.fix_gateway.builder.Validation.VALIDATION_ENABLED;

public class SessionParser
{

    private final AsciiFlyweight string = new AsciiFlyweight();
    private final LogonDecoder logon = new LogonDecoder();
    private final LogoutDecoder logout = new LogoutDecoder();
    private final RejectDecoder reject = new RejectDecoder();
    private final TestRequestDecoder testRequest = new TestRequestDecoder();
    private final HeaderDecoder header = new HeaderDecoder();
    private final SequenceResetDecoder sequenceReset = new SequenceResetDecoder();

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
                onLogon(offset, length, sessionId);
                break;

            case LogoutDecoder.MESSAGE_TYPE:
            {
                onLogout(offset, length);
                break;
            }

            case RejectDecoder.MESSAGE_TYPE:
            {
                onReject(offset, length);
                break;
            }

            case TestRequestDecoder.MESSAGE_TYPE:
            {
                onTestRequest(offset, length);
                break;
            }

            case SequenceResetDecoder.MESSAGE_TYPE:
            {
                onSequenceReset(offset, length);
                break;
            }

            default:
            {
                onAnyOtherMessage(offset, length);
                break;
            }
        }

        return session.isConnected();
    }

    private void onAnyOtherMessage(final int offset, final int length)
    {
        final HeaderDecoder header = this.header;
        header.reset();
        header.decode(string, offset, length);
        session.onMessage(header.msgSeqNum(), isPossDup(header));
    }

    private void onSequenceReset(final int offset, final int length)
    {
        sequenceReset.reset();
        sequenceReset.decode(string, offset, length);
        final HeaderDecoder header = sequenceReset.header();
        if (VALIDATION_ENABLED && !sequenceReset.validate())
        {
            session.onInvalidMessage(sequenceReset, header);
        }
        else
        {
            final boolean gapFillFlag = sequenceReset.hasGapFillFlag() && sequenceReset.gapFillFlag();
            session.onSequenceReset(
                header.msgSeqNum(),
                sequenceReset.newSeqNo(),
                gapFillFlag,
                isPossDup(header));
        }
    }

    private void onTestRequest(final int offset, final int length)
    {
        testRequest.reset();
        testRequest.decode(string, offset, length);
        final HeaderDecoder header = testRequest.header();
        if (VALIDATION_ENABLED && !testRequest.validate())
        {
            session.onInvalidMessage(testRequest, header);
        }
        else
        {
            final int msgSeqNo = header.msgSeqNum();
            session.onTestRequest(
                testRequest.testReqID(),
                testRequest.testReqIDLength(),
                msgSeqNo,
                isPossDup(header));
        }
    }

    private void onReject(final int offset, final int length)
    {
        reject.reset();
        reject.decode(string, offset, length);
        final HeaderDecoder header = reject.header();
        if (VALIDATION_ENABLED && !reject.validate())
        {
            session.onInvalidMessage(reject, header);
        }
        else
        {
            session.onReject(header.msgSeqNum(), isPossDup(header));
        }
    }

    private void onLogout(final int offset, final int length)
    {
        logout.reset();
        logout.decode(string, offset, length);
        final HeaderDecoder header = logout.header();
        if (VALIDATION_ENABLED && !logout.validate())
        {
            session.onInvalidMessage(logout, header);
        }
        else
        {
            session.onLogout(header.msgSeqNum(), isPossDup(header));
        }
    }

    private void onLogon(final int offset, final int length, final long sessionId)
    {
        logon.reset();
        logon.decode(string, offset, length);
        final HeaderDecoder header = logon.header();
        if (VALIDATION_ENABLED && !logon.validate())
        {
            session.onInvalidMessage(logon, header);
            session.requestDisconnect();
        }
        else
        {
            if (authenticationStrategy.authenticate(logon))
            {
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
                session.requestDisconnect();
            }
        }
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
