/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.fixp;

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.MutableDirectBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.messages.FixPMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import java.nio.ByteBuffer;

import static uk.co.real_logic.artio.fixp.FixPProtocol.BUSINESS_MESSAGE_LOGGING_ENABLED;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

public abstract class AbstractFixPProxy
{
    protected static final int ARTIO_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixPMessageEncoder.BLOCK_LENGTH;

    protected final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
    protected final FixPMessageEncoder fixPMessage = new FixPMessageEncoder();
    protected final BufferClaim bufferClaim = new BufferClaim();
    protected final PrintingFixPMessageConsumer fixPMessageConsumer;
    protected final ExclusivePublication publication;

    protected long connectionId;
    protected long sessionId;

    protected AbstractFixPProxy(
        final FixPProtocol protocol,
        final FixPMessageDissector dissector,
        final long connectionId,
        final ExclusivePublication publication)
    {
        this.fixPMessageConsumer = new PrintingFixPMessageConsumer(-1, protocol, dissector);
        this.connectionId = connectionId;
        this.publication = publication;
    }

    public void ids(final long connectionId, final long sessionId)
    {
        this.connectionId = connectionId;
        this.sessionId = sessionId;
    }

    public abstract long sendSequence(
        long sessionId, long nextSentSeqNo);

    public abstract long claimMessage(
        int messageLength,
        MessageEncoderFlyweight message,
        long timestamp);

    public void commit()
    {
        final BufferClaim bufferClaim = this.bufferClaim;

        if (BUSINESS_MESSAGE_LOGGING_ENABLED)
        {
            fixPMessageConsumer.onMessage(null, bufferClaim.buffer(), bufferClaim.offset(), null);
        }

        bufferClaim.commit();
    }

    public void abort()
    {
        bufferClaim.abort();
    }

    protected long claimMessage(
        final int messageLength,
        final MessageEncoderFlyweight message,
        final long timestamp,
        final int totalHeaderLength,
        final int protocolHeaderLength,
        final short protocolType)
    {
        final BufferClaim bufferClaim = this.bufferClaim;
        final long position = publication.tryClaim(totalHeaderLength + messageLength, bufferClaim);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        fixPMessage
            .wrapAndApplyHeader(buffer, bufferClaim.offset(), messageHeader)
            .connection(connectionId)
            .sessionId(sessionId)
            .enqueueTime(timestamp);

        offset += ARTIO_HEADER_LENGTH;

        SimpleOpenFramingHeader.writeSofh(buffer, offset, protocolHeaderLength + messageLength, protocolType);
        offset += SOFH_LENGTH;

        offset = applyHeader(message, buffer, offset);

        message.wrap(buffer, offset);

        return position;
    }

    protected abstract int applyHeader(MessageEncoderFlyweight message, MutableDirectBuffer buffer, int offset);

    public abstract ByteBuffer encodeReject(
        FixPContext identification, FixPFirstMessageResponse rejectReason, Enum<?> rejectCode);

    public abstract byte[] encodeFirstMessage(FixPContext context);
}
