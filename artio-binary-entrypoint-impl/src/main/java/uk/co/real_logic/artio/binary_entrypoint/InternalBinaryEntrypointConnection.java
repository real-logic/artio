/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.binary_entrypoint;

import b3.entrypoint.fixp.sbe.CancelOnDisconnectType;
import b3.entrypoint.fixp.sbe.EstablishRejectCode;
import b3.entrypoint.fixp.sbe.RetransmitRejectCode;
import b3.entrypoint.fixp.sbe.TerminationCode;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.library.FixPSessionOwner;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import static uk.co.real_logic.artio.CommonConfiguration.NO_FIXP_MAX_RETRANSMISSION_RANGE;
import static uk.co.real_logic.artio.LogTag.FIXP_SESSION;
import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;
import static uk.co.real_logic.artio.engine.logger.SequenceNumberIndexWriter.NO_REQUIRED_POSITION;
import static uk.co.real_logic.artio.fixp.FixPConnection.State.*;

/**
 * External users should never rely on this API.
 */
class InternalBinaryEntrypointConnection
    extends InternalFixPConnection implements BinaryEntrypointConnection
{
    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer(new byte[0]);

    private final BinaryEntryPointProxy proxy;
    private final BinaryEntryPointContext context;
    private final long maxFixPKeepaliveTimeoutInMs;
    private final int maxRetransmissionRange;

    private TerminationCode resendTerminationCode;

    private long sessionId;
    private long sessionVerId;
    private CancelOnDisconnectType cancelOnDisconnectType;
    private long codTimeoutWindow;
    // true iff we've sent a redact then got back-pressured sending a message after
    private boolean suppressRedactResend = false;
    private boolean suppressRetransmissionResend = false;

    InternalBinaryEntrypointConnection(
        final long connectionId,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final FixPSessionOwner owner,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final long lastConnectPayload,
        final CommonConfiguration configuration,
        final BinaryEntryPointContext context)
    {
        super(
            connectionId,
            outboundPublication,
            inboundPublication,
            libraryId,
            configuration.epochNanoClock(),
            owner,
            new BinaryEntryPointProxy(
            connectionId, outboundPublication.dataPublication(), configuration.epochNanoClock()));
        this.maxFixPKeepaliveTimeoutInMs = configuration.maxFixPKeepaliveTimeoutInMs();
        this.context = context;
        proxy = (BinaryEntryPointProxy)super.proxy;
        state(context.fromNegotiate() ? State.ACCEPTED : State.NEGOTIATED_REESTABLISH);

        final long timeInMs = System.currentTimeMillis();
        nextSendMessageTimeInMs = nextReceiveMessageTimeInMs = timeInMs + configuration.noEstablishFixPTimeoutInMs();
        // default this to the max to suppress accidentally sending sequence messages during the logon process
        requestedKeepAliveIntervalInMs = maxFixPKeepaliveTimeoutInMs;
        maxRetransmissionRange = configuration.fixPAcceptedSessionMaxRetransmissionRange();

        nextRecvSeqNo = adjustSeqNo(lastReceivedSequenceNumber);
        nextSentSeqNo = adjustSeqNo(lastSentSequenceNumber);
    }

    private long adjustSeqNo(final long lastReceivedSequenceNumber)
    {
        if (lastReceivedSequenceNumber == UNK_SESSION)
        {
            return 1;
        }

        return lastReceivedSequenceNumber + 1;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long sessionVerId()
    {
        return sessionVerId;
    }

    protected void keepAliveExpiredTerminate()
    {
        terminate(TerminationCode.UNSPECIFIED);
    }

    public long terminate(final TerminationCode terminationCode)
    {
        validateCanSend();

        return internalTerminate(terminationCode);
    }

    private long internalTerminate(final TerminationCode terminationCode)
    {
        return sendTerminate(terminationCode, State.UNBINDING, State.RESEND_TERMINATE);
    }

    public long trySendSequence()
    {
        final long position = proxy.sendSequence(sessionId, nextSentSeqNo);
        if (position > 0)
        {
            // Must check back-pressure here or we threated to suppress our retry
            onAttemptedToSendMessage();
        }
        return position;
    }

    public long tryRetransmitRequest(final long uuid, final long fromSeqNo, final int msgCount)
    {
        return 0;
    }

    public long retransmitFillSeqNo()
    {
        return 0;
    }

    public long nextRetransmitSeqNo()
    {
        return 0;
    }

    // -----------------------------------------------
    // Internal Methods below, not part of the public API
    // -----------------------------------------------

    protected int poll(final long timeInMs)
    {
        switch (state)
        {
            case ACCEPTED:
            case SENT_NEGOTIATE_RESPONSE:
            case RETRY_NEGOTIATE_RESPONSE:
            case NEGOTIATED_REESTABLISH:
                if (timeInMs > nextReceiveMessageTimeInMs)
                {
                    fullyUnbind(DisconnectReason.AUTHENTICATION_TIMEOUT);
                }
                return 1;

            case RETRY_FINISHED_SENDING:
                finishSending();
                return 1;

            default:
                return commonPoll(state, timeInMs);
        }
    }

    protected int pollExtraEstablished(final long timeInMs)
    {
        return 0;
    }

    protected long sendSequence(final boolean lapsed)
    {
        return trySendSequence();
    }

    protected void onReplayComplete()
    {
    }

    public long onNegotiate(
        final long sessionId,
        final long sessionVerID,
        final long timestamp,
        final long enteringFirm,
        final long onbehalfFirm,
        final String senderLocation)
    {
        final State state = state();
        if (!(state == State.ACCEPTED || state == State.SENT_NEGOTIATE_RESPONSE))
        {
            // TODO: validation error
        }

        onSessionId(sessionId, sessionVerID);

        // Reset sequence numbers upon successful negotiate
        nextRecvSeqNo = 1;
        nextSentSeqNo = 1;

        // Notify inbound sequence number index
        final long inboundPos = inboundPublication.saveRedactSequenceUpdate(
            sessionId, 1, NO_REQUIRED_POSITION);
        if (inboundPos < 0)
        {
            return inboundPos;
        }

        final long position = proxy.sendNegotiateResponse(sessionId, sessionVerID, timestamp, enteringFirm);
        onAttemptedToSendMessage();
        return checkState(position, State.SENT_NEGOTIATE_RESPONSE, State.RETRY_NEGOTIATE_RESPONSE);
    }

    private void onSessionId(final long sessionId, final long sessionVerID)
    {
        this.sessionId = sessionId;
        this.sessionVerId = sessionVerID;
        proxy.ids(connectionId, sessionId);
    }

    private long checkState(final long position, final State success, final State backPressured)
    {
        if (position > 0)
        {
            state(success);
        }
        else
        {
            state(backPressured);
        }
        return position;
    }

    public long onEstablish(
        final long sessionID,
        final long sessionVerID,
        final long timestamp,
        final long keepAliveIntervalInMs,
        final long nextSeqNo,
        final CancelOnDisconnectType cancelOnDisconnectType,
        final long codTimeoutWindow)
    {
        final State state = state();
        if (state == State.NEGOTIATED_REESTABLISH)
        {
            onSessionId(sessionID, sessionVerID);
        }
        else
        {
            checkSession(sessionID, sessionVerID);

            if (state != State.SENT_NEGOTIATE_RESPONSE)
            {
                onAttemptedToSendMessage();
                return proxy.sendEstablishReject(
                    sessionID,
                    sessionVerID,
                    timestamp,
                    EstablishRejectCode.ALREADY_ESTABLISHED);
            }
            else if (keepAliveIntervalInMs > maxFixPKeepaliveTimeoutInMs)
            {
                onAttemptedToSendMessage();
                final long position = proxy.sendEstablishReject(
                    sessionID,
                    sessionVerID,
                    timestamp,
                    EstablishRejectCode.KEEPALIVE_INTERVAL);
                if (position > 0)
                {
                    fullyUnbind();
                }
                return position;
            }
        }

        // Notify the inbound sequence number
        if (!suppressRedactResend)
        {
            onAttemptedToSendMessage();
            final long inboundPos = inboundPublication.saveRedactSequenceUpdate(
                sessionId, (int)nextSeqNo, NO_REQUIRED_POSITION);

            if (inboundPos > 0)
            {
                suppressRedactResend = true;
            }
            else
            {
                return inboundPos;
            }
        }

        final long position = proxy.sendEstablishAck(
            sessionID,
            sessionVerID,
            timestamp,
            keepAliveIntervalInMs,
            nextSeqNo,
            nextRecvSeqNo - 1);

        this.requestedKeepAliveIntervalInMs = keepAliveIntervalInMs;
        onAttemptedToSendMessage();
        onReceivedMessage();

        if (position > 0)
        {
            this.cancelOnDisconnectType = cancelOnDisconnectType;
            this.codTimeoutWindow = codTimeoutWindow;
            this.nextRecvSeqNo = nextSeqNo;
            this.suppressRedactResend = false;

            state(ESTABLISHED);
        }

        return position;
    }

    public long onTerminate(
        final long sessionID, final long sessionVerID, final TerminationCode terminationCode)
    {
        // We initiated termination
        if (state == State.UNBINDING)
        {
            fullyUnbind();
        }
        // The counter-party initiated termination
        else
        {
            DebugLogger.log(FIXP_SESSION, "Terminated: ", terminationCode.name());

            sendTerminateAck(terminationCode);
        }

        checkSession(sessionID, sessionVerID);

        return 1;
    }

    private void checkSession(final long sessionID, final long sessionVerID)
    {
        // TODO: log error if incorrect
    }

    private void sendTerminateAck(final TerminationCode terminationCode)
    {
        final long position = sendTerminate(terminationCode, State.UNBOUND, State.RESEND_TERMINATE_ACK);
        if (position > 0)
        {
            fullyUnbind();
        }
    }

    private long sendTerminate(
        final TerminationCode terminationCode, final State finalState, final State resendState)
    {
        final long position = proxy.sendTerminate(
            sessionId,
            sessionVerId,
            terminationCode,
            requestTimestampInNs());

        if (position > 0)
        {
            state(finalState);
            resendTerminationCode = null;
        }
        else
        {
            state(resendState);
            resendTerminationCode = terminationCode;
        }

        return position;
    }

    public long onSequence(final long nextSeqNo)
    {
        onReceivedMessage();

        return checkSeqNo(nextSeqNo);
    }

    private long checkSeqNo(final long nextSeqNo)
    {
        final long nextRecvSeqNo = this.nextRecvSeqNo;

        if (nextSeqNo > nextRecvSeqNo)
        {
            final long position = proxy.sendNotApplied(
                nextRecvSeqNo, nextSeqNo - nextRecvSeqNo, requestTimestampInNs());

            if (position > 0)
            {
                this.nextRecvSeqNo = nextSeqNo;
            }

            return position;
        }
        else if (nextSeqNo < nextRecvSeqNo)
        {
            return internalTerminate(TerminationCode.FINISHED);
        }

        return 1;
    }

    public long onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int templateId,
        final int blockLength,
        final int version,
        final int sofhMessageSize)
    {
        onReceivedMessage();

        nextRecvSeqNo++;

        handler.onBusinessMessage(
            this,
            templateId,
            buffer,
            offset,
            blockLength,
            version,
            false);

        return 1;
    }

    public void finishSending()
    {
        final long position = proxy.sendFinishedSending(
            sessionId,
            sessionVerId,
            nextRecvSeqNo - 1,
            requestTimestampInNs());
        checkState(position, SENT_FINISHED_SENDING, RETRY_FINISHED_SENDING);
    }

    public long onFinishedSending(final long sessionID, final long sessionVerID, final long lastSeqNo)
    {
        final State state = this.state;
        if (state != ESTABLISHED && state != FINISHED_SENDING)
        {
            // TODO: error
        }

        checkSession(sessionID, sessionVerID);

        // TODO: check the lastSeqNo and issue retransmit request if needed

        state(FINISHED_SENDING);

        return proxy.sendFinishedReceiving(
            sessionID,
            sessionVerID,
            requestTimestampInNs());
    }

    public long onFinishedReceiving(final long sessionID, final long sessionVerID)
    {
        final State state = this.state;
        if (state != SENT_FINISHED_SENDING && state != RETRY_FINISHED_SENDING)
        {
            // TODO: error
        }

        checkSession(sessionID, sessionVerID);

        return internalTerminate(TerminationCode.FINISHED);
    }

    public long onRetransmitRequest(
        final long sessionID, final long timestampInNs, final long fromSeqNo, final long count)
    {
        final State state = this.state;
        if (state != ESTABLISHED && state != AWAITING_KEEPALIVE)
        {
            // TODO: error
        }

        if (this.sessionId != sessionID)
        {
            return sendRetransmitReject(RetransmitRejectCode.INVALID_SESSION, timestampInNs);
        }

        if (maxRetransmissionRange != NO_FIXP_MAX_RETRANSMISSION_RANGE && count > maxRetransmissionRange)
        {
            return sendRetransmitReject(RetransmitRejectCode.REQUEST_LIMIT_EXCEEDED, timestampInNs);
        }

        final long endSequenceNumber = fromSeqNo + count - 1;
        if (endSequenceNumber >= nextSentSeqNo)
        {
            return sendRetransmitReject(RetransmitRejectCode.OUT_OF_RANGE, timestampInNs);
        }

        if (!suppressRetransmissionResend)
        {
            final long position = proxy.sendRetransmission(fromSeqNo, count, requestTimestampInNs(), timestampInNs);
            if (position < 0)
            {
                return position;
            }
        }

        final long position = inboundPublication.saveValidResendRequest(
            sessionID,
            connectionId,
            fromSeqNo,
            endSequenceNumber,
            (int)sessionVerId, // TODO
            EMPTY_BUFFER,
            0,
            0);

        // suppress if we've failed to send the resend request to the replayer as this will cause this handler to be
        // retried
        suppressRetransmissionResend = position < 0;
        return position;
    }

    private long sendRetransmitReject(final RetransmitRejectCode rejectCode, final long timestampInNs)
    {
        return proxy.sendRetransmitReject(rejectCode, requestTimestampInNs(), timestampInNs);
    }
}
