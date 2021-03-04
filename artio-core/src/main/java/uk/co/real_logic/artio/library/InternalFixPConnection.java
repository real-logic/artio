/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.library;

import io.aeron.exceptions.TimeoutException;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.fixp.FixPConnectionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import static uk.co.real_logic.artio.messages.DisconnectReason.APPLICATION_DISCONNECT;
import static uk.co.real_logic.artio.messages.DisconnectReason.LOGOUT;

public abstract class InternalFixPConnection implements FixPConnection
{
    protected final long connectionId;
    protected final GatewayPublication outboundPublication;
    protected final GatewayPublication inboundPublication;
    protected final int libraryId;
    protected final EpochNanoClock clock;
    protected final FixPSessionOwner owner;

    protected State state;
    protected FixPConnectionHandler handler;
    protected LibraryReply<InternalFixPConnection> initiateReply;

    protected InternalFixPConnection(
        final long connectionId,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final EpochNanoClock clock,
        final FixPSessionOwner owner)
    {
        this.connectionId = connectionId;
        this.outboundPublication = outboundPublication;
        this.inboundPublication = inboundPublication;
        this.libraryId = libraryId;
        this.clock = clock;
        this.owner = owner;
    }

    // -----------------------------------------------
    // Public API
    // -----------------------------------------------

    public long connectionId()
    {
        return connectionId;
    }

    public long requestDisconnect(final DisconnectReason reason)
    {
        return outboundPublication.saveRequestDisconnect(libraryId, connectionId, reason);
    }

    public boolean canSendMessage()
    {
        final State state = this.state;
        return state == State.ESTABLISHED || state == State.AWAITING_KEEPALIVE;
    }

    public State state()
    {
        return state;
    }

    // -----------------------------------------------
    // Internal Methods below, not part of the public API
    // -----------------------------------------------

    protected void validateCanSend()
    {
        if (!canSendMessage())
        {
            throw new IllegalStateException(
                "State should be ESTABLISHED or AWAITING_KEEPALIVE in order to send but is " + state);
        }
    }

    @SuppressWarnings("unchecked")
    protected void initiateReply(final LibraryReply<?> initiateReply)
    {
        this.initiateReply = (LibraryReply<InternalFixPConnection>)initiateReply;
    }

    protected void onNegotiateFailure()
    {
        onReplyError(new TimeoutException("Timed out: no reply for Negotiate"));
    }

    protected void onEstablishFailure()
    {
        onReplyError(new TimeoutException("Timed out: no reply for Establish"));
    }

    protected void onReplyError(final Exception error)
    {
        initiateReply.onError(error);
        initiateReply = null;
    }

    protected void fullyUnbind()
    {
        requestDisconnect(LOGOUT);
        owner.remove(this);
        unbindState(APPLICATION_DISCONNECT);
    }

    protected void unbindState(final DisconnectReason reason)
    {
        state = State.UNBOUND;
        handler.onDisconnect(this, reason);

        // Complete the reply if we're in the process of trying to establish a connection and we haven't provided
        // a more specific reason for a disconnect to happen.
        if (initiateReply != null)
        {
            onReplyError(new Exception("Unbound due to: " + reason));
        }
    }

    protected void state(final State state)
    {
        this.state = state;
    }

    public void handler(final FixPConnectionHandler handler)
    {
        this.handler = handler;
    }

    protected abstract int poll(long timeInMs);

    protected abstract void onReplayComplete();

}
