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
import b3.entrypoint.fixp.sbe.DeltaInMillisDecoder;
import b3.entrypoint.fixp.sbe.TerminationCode;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.fixp.FixPConnectionHandler;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;

/**
 * External users should never rely on this API.
 */
public class InternalBinaryEntrypointConnection
    extends InternalFixPConnection implements BinaryEntrypointConnection
{
    private final BinaryEntryPointProxy proxy;
    private final EpochNanoClock clock;

    private FixPConnectionHandler handler;

    public InternalBinaryEntrypointConnection(
        final long connectionId,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final long lastConnectPayload,
        final EpochNanoClock clock)
    {
        this.clock = clock;
        proxy = new BinaryEntryPointProxy(connectionId, outboundPublication.dataPublication());
    }

    public int sessionId()
    {
        return 0;
    }

    public long sessionVerId()
    {
        return 0;
    }

    public long tryClaim(final MessageEncoderFlyweight message)
    {
        return 0;
    }

    public long tryClaim(final MessageEncoderFlyweight message, final int variableLength)
    {
        return 0;
    }

    public void commit()
    {
    }

    public void abort()
    {

    }

    public long trySendSequence()
    {
        return 0;
    }

    public long requestDisconnect(final DisconnectReason reason)
    {
        return 0;
    }

    public long terminate(final String shutdown, final int errorCodes)
    {
        return 0;
    }

    public long tryRetransmitRequest(final long uuid, final long fromSeqNo, final int msgCount)
    {
        return 0;
    }

    public long connectionId()
    {
        return 0;
    }

    public State state()
    {
        return null;
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

    public boolean canSendMessage()
    {
        return false;
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

    protected void fullyUnbind()
    {
    }

    protected void unbindState(final DisconnectReason reason)
    {
    }

    public void handler(final FixPConnectionHandler handler)
    {
        this.handler = handler;
    }

    public long onNegotiate(
        final long sessionID,
        final long sessionVerID,
        final long timestamp,
        final long enteringFirm,
        final long onbehalfFirm,
        final String senderLocation)
    {
        return proxy.sendNegotiateResponse(sessionID, sessionVerID, timestamp, enteringFirm);
    }

    private long requestTimestamp()
    {
        return clock.nanoTime();
    }

    public long onEstablish(
        final long sessionID,
        final long sessionVerID,
        final long timestamp,
        final long keepAliveInterval,
        final long nextSeqNo,
        final CancelOnDisconnectType cancelOnDisconnectType,
        final DeltaInMillisDecoder codTimeoutWindow)
    {
        return proxy.sendEstablishAck(
            sessionID,
            sessionVerID,
            timestamp,
            keepAliveInterval,
            1,
            0);
    }

    public long onTerminate(final long sessionID, final long sessionVerID, final TerminationCode terminationCode)
    {
        return 0;
    }

    public long onSequence(final long nextSeqNo)
    {
        return 0;
    }
}
