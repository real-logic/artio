/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.messages.ConnectionType;
import uk.co.real_logic.artio.messages.SlowStatus;
import uk.co.real_logic.artio.session.*;

import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.artio.LogTag.GATEWAY_MESSAGE;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;

class GatewaySession implements SessionInfo
{
    private static final int NO_TIMEOUT = -1;

    private final long connectionId;
    private SessionContext context;
    private final String address;
    private final ConnectionType connectionType;
    private final boolean closedResendInterval;
    private final int resendRequestChunkSize;
    private final boolean sendRedundantResendRequests;
    private final boolean enableLastMsgSeqNumProcessed;
    private final FixDictionary fixDictionary;
    private final long authenticationTimeoutInMs;

    private ReceiverEndPoint receiverEndPoint;
    private SenderEndPoint senderEndPoint;

    private long sessionId;
    private SessionParser sessionParser;
    private InternalSession session;
    private CompositeKey sessionKey;
    private String username;
    private String password;
    private int heartbeatIntervalInS;
    private long disconnectTimeInMs = NO_TIMEOUT;

    private Consumer<GatewaySession> onGatewaySessionLogon;
    private SessionLogonListener logonListener = this::onSessionLogon;
    private boolean initialResetSeqNum;
    private boolean hasStartedAuthentication = false;
    private int logonReceivedSequenceNumber;
    private int logonSequenceIndex;

    GatewaySession(
        final long connectionId,
        final SessionContext context,
        final String address,
        final ConnectionType connectionType,
        final CompositeKey sessionKey,
        final ReceiverEndPoint receiverEndPoint,
        final SenderEndPoint senderEndPoint,
        final Consumer<GatewaySession> onGatewaySessionLogon,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary,
        final long authenticationTimeoutInMs)
    {
        this.connectionId = connectionId;
        this.sessionId = context.sessionId();
        this.context = context;
        this.address = address;
        this.connectionType = connectionType;
        this.sessionKey = sessionKey;
        this.receiverEndPoint = receiverEndPoint;
        this.senderEndPoint = senderEndPoint;
        this.onGatewaySessionLogon = onGatewaySessionLogon;
        this.closedResendInterval = closedResendInterval;
        this.resendRequestChunkSize = resendRequestChunkSize;
        this.sendRedundantResendRequests = sendRedundantResendRequests;
        this.enableLastMsgSeqNumProcessed = enableLastMsgSeqNumProcessed;
        this.fixDictionary = fixDictionary;
        this.authenticationTimeoutInMs = authenticationTimeoutInMs;
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

    void manage(
        final SessionParser sessionParser,
        final InternalSession session,
        final BlockablePosition blockablePosition)
    {
        this.sessionParser = sessionParser;
        this.session = session;
        this.session.logonListener(logonListener);
        receiverEndPoint.libraryId(ENGINE_LIBRARY_ID);
        senderEndPoint.libraryId(ENGINE_LIBRARY_ID, blockablePosition);
    }

    // sets management to a library and also cleans up locally associated session.
    void handoverManagementTo(
        final int libraryId,
        final BlockablePosition blockablePosition)
    {
        setManagementTo(libraryId, blockablePosition);

        sessionParser = null;
        session.logonListener(null);
        context.updateAndSaveFrom(session);
        session.close();
        session = null;
    }

    void setManagementTo(final int libraryId, final BlockablePosition blockablePosition)
    {
        receiverEndPoint.libraryId(libraryId);
        receiverEndPoint.pause();
        senderEndPoint.libraryId(libraryId, blockablePosition);
    }

    void play()
    {
        receiverEndPoint.play();
    }

    int poll(final long timeInMs)
    {
        final int events = session != null ? session.poll(timeInMs) : 0;
        return events + checkNoLogonDisconnect(timeInMs);
    }

    private int checkNoLogonDisconnect(final long timeInMs)
    {
        if (disconnectTimeInMs == NO_TIMEOUT)
        {
            return 0;
        }

        if (disconnectTimeInMs <= timeInMs && !receiverEndPoint.hasDisconnected())
        {
            if (hasStartedAuthentication)
            {
                receiverEndPoint.onAuthenticationTimeoutDisconnect();
            }
            else
            {
                receiverEndPoint.onNoLogonDisconnect();
            }
            return 1;
        }

        return 0;
    }

    void startAuthentication(final long timeInMs)
    {
        hasStartedAuthentication = true;
        disconnectTimeInMs = timeInMs + authenticationTimeoutInMs;
    }

    void onAuthenticationResult()
    {
        disconnectTimeInMs = NO_TIMEOUT;
    }

    private void onSessionLogon(final Session session)
    {
        context.updateFrom(session);
        onGatewaySessionLogon.accept(this);
    }

    InternalSession session()
    {
        return session;
    }

    ConnectionType connectionType()
    {
        return connectionType;
    }

    public void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long messageType,
        final long sessionId)
    {
        if (sessionParser != null)
        {
            DebugLogger.log(FIX_MESSAGE, "Gateway Received %s %n", buffer, offset, length);

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
            DebugLogger.log(GATEWAY_MESSAGE, "Setup Session As: %s%n", sessionKey.localCompId());
        }
        senderEndPoint.sessionId(sessionId);
    }

    public void onLogon(
        final long sessionId,
        final SessionContext context,
        final CompositeKey sessionKey,
        final String username,
        final String password,
        final int heartbeatIntervalInS,
        final int logonReceivedSequenceNumber)
    {
        this.sessionId = sessionId;
        this.context = context;
        this.sessionKey = sessionKey;
        this.logonReceivedSequenceNumber = logonReceivedSequenceNumber;
        this.logonSequenceIndex = context.sequenceIndex();

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

    void acceptorSequenceNumbers(final int retrievedSentSequenceNumber, final int retrievedReceivedSequenceNumber)
    {
        if (session != null)
        {
            session.lastSentMsgSeqNum(adjustLastSequenceNumber(retrievedSentSequenceNumber));
            session.initialLastReceivedMsgSeqNum(adjustLastSequenceNumber(retrievedReceivedSequenceNumber));
        }
    }

    static int adjustLastSequenceNumber(final int lastSequenceNumber)
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
        this.disconnectTimeInMs = disconnectTimeout;
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

    SlowStatus slowStatus()
    {
        return bytesInBuffer() > 0 ? SlowStatus.SLOW : SlowStatus.NOT_SLOW;
    }

    public boolean closedResendInterval()
    {
        return closedResendInterval;
    }

    public int resendRequestChunkSize()
    {
        return resendRequestChunkSize;
    }

    public boolean sendRedundantResendRequests()
    {
        return sendRedundantResendRequests;
    }

    public boolean enableLastMsgSeqNumProcessed()
    {
        return enableLastMsgSeqNumProcessed;
    }

    public SessionContext context()
    {
        return context;
    }

    boolean hasDisconnected()
    {
        return receiverEndPoint.hasDisconnected();
    }

    void initialResetSeqNum(final boolean resetSeqNum)
    {
        initialResetSeqNum = resetSeqNum;
    }

    boolean initialResetSeqNum()
    {
        return initialResetSeqNum;
    }

    FixDictionary fixDictionary()
    {
        return fixDictionary;
    }

    public int logonReceivedSequenceNumber()
    {
        return logonReceivedSequenceNumber;
    }

    public int logonSequenceIndex()
    {
        return logonSequenceIndex;
    }
}
