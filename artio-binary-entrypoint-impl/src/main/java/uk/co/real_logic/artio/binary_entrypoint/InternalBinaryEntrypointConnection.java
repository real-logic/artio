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

import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.messages.DisconnectReason;

/**
 * External users should never rely on this API.
 */
public class InternalBinaryEntrypointConnection
    extends InternalFixPConnection implements BinaryEntrypointConnection
{
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
}
