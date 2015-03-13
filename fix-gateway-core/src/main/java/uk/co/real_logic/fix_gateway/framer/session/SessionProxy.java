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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

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
    private final Publication dataPublication;
    private final SessionIdStrategy sessionIdStrategy;

    public SessionProxy(
        final int bufferSize, final Publication dataPublication, final SessionIdStrategy sessionIdStrategy)
    {
        this.sessionIdStrategy = sessionIdStrategy;
        buffer = new UnsafeBuffer(new byte[bufferSize]);
        string = new MutableAsciiFlyweight(buffer);
        this.dataPublication = dataPublication;
    }

    public void resendRequest(final int beginSeqNo, final int endSeqNo)
    {
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
        send(resendRequest.encode(string, 0));
    }

    public void disconnect(final long connectionId)
    {
        // TODO
    }

    public void logon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        final HeaderEncoder header = logon.header();
        sessionIdStrategy.encode(sessionId, header);
        header.msgSeqNum(msgSeqNo);

        logon.heartBtInt(heartbeatInterval);
        send(logon.encode(string, 0));
    }

    public void logout(final int msgSeqNo, final long sessionId)
    {
        final HeaderEncoder header = logout.header();
        sessionIdStrategy.encode(sessionId, header);

        logout.header().msgSeqNum(msgSeqNo);
        send(logout.encode(string, 0));
    }

    public void heartbeat(final String testReqId)
    {
        heartbeat.testReqID(testReqId);
        send(heartbeat.encode(string, 0));
    }

    public void reject(final int msgSeqNo, final int refSeqNum)
    {
        reject.header().msgSeqNum(msgSeqNo);
        reject.refSeqNum(refSeqNum);
        // TODO: decide on other ref fields
        send(reject.encode(string, 0));
    }

    private void send(final int length)
    {
        while (!dataPublication.offer(buffer, 0, length))
        {
            // TODO: backoff.
        }
        System.out.println("buffered");
    }
}
