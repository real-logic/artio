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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.FixPublication;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.FixPublication.FRAME_SIZE;

/**
 * Encapsulates sending messages relating to sessions
 */
public class SessionProxy
{

    private final LogonEncoder logon = new LogonEncoder();
    private final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();
    private final HeartbeatEncoder heartbeat = new HeartbeatEncoder();
    private final RejectEncoder reject = new RejectEncoder();

    private final UnsafeBuffer buffer;
    private final MutableAsciiFlyweight string;
    private final FixPublication fixPublication;
    private final SessionIdStrategy sessionIdStrategy;

    public SessionProxy(
        final int bufferSize, final FixPublication fixPublication, final SessionIdStrategy sessionIdStrategy)
    {
        this.fixPublication = fixPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        buffer = new UnsafeBuffer(new byte[bufferSize]);
        string = new MutableAsciiFlyweight(buffer);
    }

    public void resendRequest(final int msgSeqNo, final int beginSeqNo, final int endSeqNo, final long sessionId)
    {
        final HeaderEncoder header = resendRequest.header();
        sessionIdStrategy.encode(sessionId, header);
        header.msgSeqNum(msgSeqNo);
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
        send(resendRequest.encode(string, FRAME_SIZE), sessionId, ResendRequestDecoder.MESSAGE_TYPE);
    }

    public void disconnect(final long connectionId)
    {
        // TODO
        System.out.println("DISCONNECTING: " + connectionId);
    }

    public void logon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        System.out.println("Sending logon to : " + sessionId);
        final HeaderEncoder header = logon.header();
        sessionIdStrategy.encode(sessionId, header);
        header.msgSeqNum(msgSeqNo);

        logon.heartBtInt(heartbeatInterval);
        send(logon.encode(string, FRAME_SIZE), sessionId, LogonDecoder.MESSAGE_TYPE);
    }

    public void logout(final int msgSeqNo, final long sessionId)
    {
        final HeaderEncoder header = logout.header();
        sessionIdStrategy.encode(sessionId, header);
        header.msgSeqNum(msgSeqNo);

        send(logout.encode(string, FRAME_SIZE), sessionId, LogoutDecoder.MESSAGE_TYPE);
    }

    public void heartbeat(final String testReqId, final long sessionId)
    {
        final HeaderEncoder header = heartbeat.header();
        sessionIdStrategy.encode(sessionId, header);
        // TODO: header.msgSeqNum(0);

        if (testReqId != null)
        {
            heartbeat.testReqID(testReqId);
        }
        send(heartbeat.encode(string, FRAME_SIZE), sessionId, HeartbeatDecoder.MESSAGE_TYPE);
    }

    public void reject(final int msgSeqNo, final int refSeqNum, final long sessionId)
    {
        final HeaderEncoder header = reject.header();
        sessionIdStrategy.encode(sessionId, header);
        header.msgSeqNum(msgSeqNo);

        reject.refSeqNum(refSeqNum);
        // TODO: decide on other ref fields
        send(reject.encode(string, FRAME_SIZE), sessionId, RejectDecoder.MESSAGE_TYPE);
    }

    private void send(final int length, final long sessionId, final int messageType)
    {
        System.out.println("Session Proxy: ");
        string.log(FRAME_SIZE, length);
        fixPublication.onMessage(buffer, 0, length + FRAME_SIZE, sessionId, messageType);
    }
}
