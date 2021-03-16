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
import b3.entrypoint.fixp.sbe.TerminationCode;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.library.FixPSessionOwner;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import static uk.co.real_logic.artio.LogTag.FIXP_SESSION;

/**
 * External users should never rely on this API.
 */
class InternalBinaryEntrypointConnection
    extends InternalFixPConnection implements BinaryEntrypointConnection
{
    private final BinaryEntryPointProxy proxy;
    private final BinaryEntryPointContext context;

    private TerminationCode resendTerminationCode;

    private long sessionId;
    private long sessionVerId;
    private CancelOnDisconnectType cancelOnDisconnectType;
    private long codTimeoutWindow;

    InternalBinaryEntrypointConnection(
        final long connectionId,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final FixPSessionOwner owner,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final long lastConnectPayload,
        final EpochNanoClock clock,
        final BinaryEntryPointContext context)
    {
        super(
            connectionId,
            outboundPublication,
            inboundPublication,
            libraryId,
            clock,
            owner,
            new BinaryEntryPointProxy(connectionId, outboundPublication.dataPublication()));
        this.context = context;
        proxy = (BinaryEntryPointProxy)super.proxy;
        state(context.fromNegotiate() ? State.ACCEPTED : State.NEGOTIATED_REESTABLISH);
    }

    public long sessionId()
    {
        return sessionId;
    }

    public long sessionVerId()
    {
        return sessionVerId;
    }

    public long terminate(final TerminationCode terminationCode)
    {
        validateCanSend();

        return sendTerminate(terminationCode, State.UNBINDING, State.RESEND_TERMINATE);
    }

    public long trySendSequence()
    {
        return 0;
    }

    public long tryRetransmitRequest(final long uuid, final long fromSeqNo, final int msgCount)
    {
        return 0;
    }

    public long nextSentSeqNo()
    {
        return 0;
    }

    public void nextSentSeqNo(final long nextSentSeqNo)
    {

    }

    public long nextRecvSeqNo()
    {
        return 0;
    }

    public void nextRecvSeqNo(final long nextRecvSeqNo)
    {

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
        return 0;
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

        this.sessionId = sessionId;
        this.sessionVerId = sessionVerID;

        final long position = proxy.sendNegotiateResponse(sessionId, sessionVerID, timestamp, enteringFirm);
        return checkState(position, State.SENT_NEGOTIATE_RESPONSE, State.RETRY_NEGOTIATE_RESPONSE);
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
        final long keepAliveInterval,
        final long nextSeqNo,
        final CancelOnDisconnectType cancelOnDisconnectType,
        final long codTimeoutWindow)
    {
        final State state = state();
        if (state == State.NEGOTIATED_REESTABLISH)
        {
            this.sessionId = sessionID;
            this.sessionVerId = sessionVerID;
        }
        else
        {
            checkSession(sessionID, sessionVerID);

            if (state != State.SENT_NEGOTIATE_RESPONSE)
            {
                // TODO: validation error
            }
        }

        this.cancelOnDisconnectType = cancelOnDisconnectType;
        this.codTimeoutWindow = codTimeoutWindow;

        final long position = proxy.sendEstablishAck(
            sessionID,
            sessionVerID,
            timestamp,
            keepAliveInterval,
            1,
            0);
        return checkState(position, State.ESTABLISHED, State.RETRY_ESTABLISH_ACK);
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
            state = finalState;
            resendTerminationCode = null;
        }
        else
        {
            state = resendState;
            resendTerminationCode = terminationCode;
        }

        return position;
    }

    public long onSequence(final long nextSeqNo)
    {
        return 0;
    }

    public long onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int templateId,
        final int blockLength,
        final int version,
        final int sofhMessageSize)
    {
//        onReceivedMessage();

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
}
