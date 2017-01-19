/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionParser;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static uk.co.real_logic.fix_gateway.LogTag.GATEWAY_MESSAGE;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;

class GatewaySession implements SessionInfo
{
    private static final int NO_TIMEOUT = -1;

    private final long connectionId;
    private SessionContext context;
    private final String address;
    private final ConnectionType connectionType;

    private ReceiverEndPoint receiverEndPoint;
    private SenderEndPoint senderEndPoint;

    private long sessionId;
    private SessionParser sessionParser;
    private Session session;
    private CompositeKey sessionKey;
    private String username;
    private String password;
    private int heartbeatIntervalInS;
    private long disconnectTimeout = NO_TIMEOUT;

    GatewaySession(final long connectionId,
                   final SessionContext context,
                   final String address,
                   final ConnectionType connectionType,
                   final CompositeKey sessionKey,
                   final ReceiverEndPoint receiverEndPoint,
                   final SenderEndPoint senderEndPoint)
    {
        this.connectionId = connectionId;
        this.sessionId = context.sessionId();
        this.context = context;
        this.address = address;
        this.connectionType = connectionType;
        this.sessionKey = sessionKey;
        this.receiverEndPoint = receiverEndPoint;
        this.senderEndPoint = senderEndPoint;
    }

    public long connectionId()
    {
        return connectionId;
    }

    public String address()
    {
        return address;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public CompositeKey sessionKey()
    {
        return sessionKey;
    }

    void manage(final SessionParser sessionParser, final Session session)
    {
        this.sessionParser = sessionParser;
        this.session = session;
        receiverEndPoint.libraryId(ENGINE_LIBRARY_ID);
        senderEndPoint.libraryId(ENGINE_LIBRARY_ID);
    }

    void handoverManagementTo(final int libraryId)
    {
        receiverEndPoint.libraryId(libraryId);
        receiverEndPoint.pause();
        senderEndPoint.libraryId(libraryId);
        sessionParser = null;
        context.updateFrom(session);
        session.close();
        session = null;
    }

    void play()
    {
        receiverEndPoint.play();
    }

    int poll(final long time)
    {
        return session.poll(time) + checkNoLogonDisconnect(time);
    }

    private int checkNoLogonDisconnect(final long time)
    {
        if (disconnectTimeout == NO_TIMEOUT)
        {
            return 0;
        }

        if (sessionKey != null)
        {
            disconnectTimeout = NO_TIMEOUT;
            return 1;
        }

        if (disconnectTimeout <= time && !receiverEndPoint.hasDisconnected())
        {
            receiverEndPoint.onNoLogonDisconnect();
            return 1;
        }

        return 0;
    }

    Session session()
    {
        return session;
    }

    ConnectionType connectionType()
    {
        return connectionType;
    }

    public void onMessage(final MutableAsciiBuffer buffer,
                          final int offset,
                          final int length,
                          final int messageType,
                          final long sessionId)
    {
        if (sessionParser != null)
        {
            sessionParser.onMessage(buffer, offset, length, messageType, sessionId);
        }
    }

    void onLogon(
        final String username,
        final String password,
        final int heartbeatIntervalInS)
    {
        this.username = username;
        this.password = password;
        this.heartbeatIntervalInS = heartbeatIntervalInS;
        if (session != null)
        {
            session.setupSession(sessionId, sessionKey);
            sessionParser.sequenceIndex(context.sequenceIndex());
            DebugLogger.log(GATEWAY_MESSAGE, "Setup Session As: %s\n", sessionKey.senderCompId());
        }
        senderEndPoint.onLogon(sessionId);
    }

    public void onLogon(
        final long sessionId,
        final SessionContext context,
        final CompositeKey sessionKey,
        final String username,
        final String password,
        final int heartbeatIntervalInS)
    {
        this.sessionId = sessionId;
        this.context = context;
        this.sessionKey = sessionKey;
        onLogon(username, password, heartbeatIntervalInS);
    }

    public String username()
    {
        return username;
    }

    public String password()
    {
        return password;
    }

    int heartbeatIntervalInS()
    {
        return heartbeatIntervalInS;
    }

    void acceptorSequenceNumbers(final int sentSequenceNumber, final int receivedSequenceNumber)
    {
        if (session != null)
        {
            session.lastSentMsgSeqNum(adjustLastSequenceNumber(sentSequenceNumber));
            session.lastReceivedMsgSeqNum(adjustLastSequenceNumber(receivedSequenceNumber));
        }
    }

    private int adjustLastSequenceNumber(final int lastSequenceNumber)
    {
        return (lastSequenceNumber == UNK_SESSION) ? 0 : lastSequenceNumber;
    }

    public String toString()
    {
        return "GatewaySession{" +
            "sessionId=" + sessionId +
            ", sessionKey=" + sessionKey +
            '}';
    }

    void disconnectAt(final long disconnectTimeout)
    {
        this.disconnectTimeout = disconnectTimeout;
    }

    public long bytesInBuffer()
    {
        return senderEndPoint.bytesInBuffer();
    }

    void close()
    {
        session.close();
    }

    int sequenceIndex()
    {
        return context.sequenceIndex();
    }
}
