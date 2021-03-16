/*
 * Copyright 2015-2020 Real Logic Limited.
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

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.util.AsciiBuffer;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.artio.LogTag.GATEWAY_MESSAGE;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.messages.CancelOnDisconnectOption.DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT;

class FixGatewaySession extends GatewaySession implements ConnectedSessionInfo, SessionProcessHandler
{
    private final boolean closedResendInterval;
    private final int resendRequestChunkSize;
    private final boolean sendRedundantResendRequests;
    private final boolean enableLastMsgSeqNumProcessed;
    private final EngineConfiguration configuration;

    private FixDictionary fixDictionary;
    private FixReceiverEndPoint receiverEndPoint;
    private FixSenderEndPoint senderEndPoint;

    private SessionContext context;
    private SessionParser sessionParser;
    private InternalSession session;
    private DirectSessionProxy proxy;
    private CompositeKey sessionKey;
    private String username;
    private String password;
    private int heartbeatIntervalInS;

    private Consumer<FixGatewaySession> onGatewaySessionLogon;
    private boolean initialResetSeqNum;
    private int logonReceivedSequenceNumber;
    private int logonSequenceIndex;
    // lastLogonTime is set when the logon message is processed
    // when we process the logon, the lastSequenceResetTime is set if it does reset the sequence.
    // Otherwise this is updated when we handover the session.
    private long lastSequenceResetTime = Session.UNKNOWN_TIME;
    private long lastLogonTime = Session.UNKNOWN_TIME;
    private CancelOnDisconnectOption cancelOnDisconnectOption;
    private long cancelOnDisconnectTimeoutWindowInNs;

    FixGatewaySession(
        final long connectionId,
        final SessionContext context,
        final String address,
        final ConnectionType connectionType,
        final CompositeKey sessionKey,
        final FixReceiverEndPoint receiverEndPoint,
        final FixSenderEndPoint senderEndPoint,
        final Consumer<FixGatewaySession> onGatewaySessionLogon,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary,
        final EngineConfiguration configuration)
    {
        super(connectionId, context.sessionId(), address, connectionType, configuration.authenticationTimeoutInMs());
        this.context = context;
        this.sessionKey = sessionKey;
        this.receiverEndPoint = receiverEndPoint;
        this.senderEndPoint = senderEndPoint;
        this.onGatewaySessionLogon = onGatewaySessionLogon;
        this.closedResendInterval = closedResendInterval;
        this.resendRequestChunkSize = resendRequestChunkSize;
        this.sendRedundantResendRequests = sendRedundantResendRequests;
        this.enableLastMsgSeqNumProcessed = enableLastMsgSeqNumProcessed;
        this.configuration = configuration;
        fixDictionary(fixDictionary);
    }

    public String address()
    {
        if (receiverEndPoint != null)
        {
            return receiverEndPoint.address();
        }

        return address;
    }

    public CompositeKey sessionKey()
    {
        return sessionKey;
    }

    void manage(
        final SessionParser sessionParser,
        final InternalSession session,
        final BlockablePosition blockablePosition,
        final DirectSessionProxy proxy)
    {
        this.sessionParser = sessionParser;
        this.session = session;
        this.proxy = proxy;
        this.session.sessionProcessHandler(this);
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
        session.sessionProcessHandler(null);
        context.updateAndSaveFrom(session);
        session.close();
        session = null;
        proxy = null;
    }

    void setManagementTo(final int libraryId, final BlockablePosition blockablePosition)
    {
        libraryId(libraryId);
        receiverEndPoint.libraryId(libraryId);
        receiverEndPoint.pause();
        senderEndPoint.libraryId(libraryId, blockablePosition);
    }

    void play()
    {
        if (receiverEndPoint != null)
        {
            receiverEndPoint.play();
        }
    }

    int poll(final long timeInMs, final long timeInNs)
    {
        final int events = session != null ? session.poll(timeInNs) : 0;
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

    void onAuthenticationResult()
    {
        disconnectTimeInMs = NO_TIMEOUT;
    }

    public void onLogon(final Session session)
    {
        context.updateFrom(session);
        onGatewaySessionLogon.accept(this);
    }

    public Reply<ReplayMessagesStatus> replayReceivedMessages(
        final long sessionId,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final int replayToSequenceNumber,
        final int replayToSequenceIndex,
        final long timeout)
    {
        return unsupported();
    }

    public void enqueueTask(final BooleanSupplier task)
    {
        unsupported();
    }

    public Reply<ThrottleConfigurationStatus> messageThrottle(
        final long sessionId, final int throttleWindowInMs, final int throttleLimitOfMessages)
    {
        return unsupported();
    }

    private <T> T unsupported()
    {
        throw new UnsupportedOperationException("Should never be invoked inside the Engine.");
    }

    InternalSession session()
    {
        return session;
    }

    public void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long messageType,
        final long position)
    {
        if (sessionParser != null)
        {
            DebugLogger.log(FIX_MESSAGE, "Gateway Received ", buffer, offset, length);

            session.messageInfo().isValid(true);

            sessionParser.onMessage(buffer, offset, length, messageType, position);
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
            sessionParser.sessionKey(sessionKey);
            sessionParser.sequenceIndex(context.sequenceIndex());
            DebugLogger.log(GATEWAY_MESSAGE, "Setup Session As: ", sessionKey.localCompId());
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
        final int logonReceivedSequenceNumber,
        final CancelOnDisconnectOption cancelOnDisconnectOption,
        final long cancelOnDisconnectTimeoutWindowInNs)
    {
        this.sessionId = sessionId;
        this.context = context;
        this.sessionKey = sessionKey;
        this.logonReceivedSequenceNumber = logonReceivedSequenceNumber;
        this.logonSequenceIndex = context.sequenceIndex();
        this.cancelOnDisconnectOption = cancelOnDisconnectOption;
        this.cancelOnDisconnectTimeoutWindowInNs = cancelOnDisconnectTimeoutWindowInNs;

        senderEndPoint.onLogon(sessionKey, configuration);

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
            final int lastReceivedMsgSeqNum = adjustLastSequenceNumber(retrievedReceivedSequenceNumber);
            session.initialLastReceivedMsgSeqNum(lastReceivedMsgSeqNum);
        }
    }

    void lastLogonWasSequenceReset()
    {
        lastSequenceResetTime(lastLogonTime);
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

    public long bytesInBuffer()
    {
        return senderEndPoint.bytesInBuffer();
    }

    void close()
    {
        CloseHelper.close(session);
    }

    public int sequenceIndex()
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

    void fixDictionary(final FixDictionary fixDictionary)
    {
        this.fixDictionary = fixDictionary;
        if (senderEndPoint != null)
        {
            senderEndPoint.fixDictionary(fixDictionary);
        }
    }

    public int logonReceivedSequenceNumber()
    {
        return logonReceivedSequenceNumber;
    }

    public int logonSequenceIndex()
    {
        return logonSequenceIndex;
    }

    public CancelOnDisconnectOption cancelOnDisconnectOption()
    {
        return cancelOnDisconnectOption == null ? DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT : cancelOnDisconnectOption;
    }

    public long cancelOnDisconnectTimeoutWindowInNs()
    {
        return cancelOnDisconnectTimeoutWindowInNs;
    }

    void updateSessionDictionary()
    {
        if (session != null)
        {
            session.fixDictionary(fixDictionary);
            sessionParser.fixDictionary(fixDictionary);
        }
    }

    long lastSequenceResetTime()
    {
        return lastSequenceResetTime;
    }

    void lastSequenceResetTime(final long lastSequenceResetTime)
    {
        this.lastSequenceResetTime = lastSequenceResetTime;
        if (session != null)
        {
            session.lastSequenceResetTimeInNs(lastSequenceResetTime);
        }
    }

    long lastLogonTime()
    {
        return lastLogonTime;
    }

    void lastLogonTime(final long lastLogonTime)
    {
        this.lastLogonTime = lastLogonTime;
        if (session != null)
        {
            session.lastLogonTimeInNs(lastLogonTime);
        }
    }

    public boolean isOffline()
    {
        return receiverEndPoint == null;
    }

    public void goOffline()
    {
        // Library retains ownership of a disconnected session, reset state to that of an offline GatewaySession object
        connectionId = NO_CONNECTION_ID;
        address = ":" + NO_CONNECTION_ID;
        receiverEndPoint = null;
        senderEndPoint = null;
        onGatewaySessionLogon = null;
    }

    public long lastSentPosition()
    {
        return proxy.lastSentPosition();
    }

    public void onDisconnectReleasedByOwner()
    {
        if (session != null)
        {
            session.onDisconnect();
        }
    }

    public boolean onThrottleNotification(
        final long messageType,
        final int refSeqNum,
        final AsciiBuffer refIdBuffer,
        final int refIdOffset,
        final int refIdLength)
    {
        if (session != null)
        {
            return session.onThrottleNotification(
                messageType,
                refSeqNum,
                refIdBuffer,
                refIdOffset,
                refIdLength
            );
        }

        return true;
    }

    public boolean configureThrottle(final int throttleWindowInMs, final int throttleLimitOfMessages)
    {
        final boolean ok = senderEndPoint.configureThrottle(throttleWindowInMs, throttleLimitOfMessages);
        if (ok)
        {
            receiverEndPoint.configureThrottle(throttleWindowInMs, throttleLimitOfMessages);
        }
        return ok;
    }
}
