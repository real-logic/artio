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
package uk.co.real_logic.artio.ilink;

import iLinkBinary.Negotiate500Encoder;
import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.MutableDirectBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.messages.ILinkMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

public class ILink3Proxy extends AbstractILink3Proxy
{
    public static final int ILINK_MESSAGE_HEADER = MessageHeaderEncoder.ENCODED_LENGTH +
        ILinkMessageEncoder.BLOCK_LENGTH;

    private final ILinkMessageEncoder iLinkMessage = new ILinkMessageEncoder();
    private final BufferClaim bufferClaim = new BufferClaim();

    private final long connectionId;
    private final ExclusivePublication publication;
    private final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
    private final iLinkBinary.MessageHeaderEncoder iLinkMessageHeader = new iLinkBinary.MessageHeaderEncoder();

    private final Negotiate500Encoder negotiate = new Negotiate500Encoder();

    public ILink3Proxy(final long connectionId, final int encodeBufferSize, final ExclusivePublication publication)
    {
        this.connectionId = connectionId;

        this.publication = publication;
    }

    public long sendNegotiate(
        final byte[] hMACSignature,
        final String accessKeyId,
        final long uuid,
        final long requestTimestamp,
        final String sessionId,
        final String firmId)
    {
        final Negotiate500Encoder negotiate = this.negotiate;

        final long position = claimILinkMessage(Negotiate500Encoder.BLOCK_LENGTH, negotiate);
        if (position < 0)
        {
            return position;
        }

        negotiate
            .putHMACSignature(hMACSignature, 0)
            .accessKeyID(accessKeyId)
            .uUID(uuid)
            .requestTimestamp(requestTimestamp)
            .session(sessionId)
            .firm(firmId);

        return position;
    }

    public long claimILinkMessage(
        final int length,
        final MessageEncoderFlyweight message)
    {
        final BufferClaim bufferClaim = this.bufferClaim;
        final long position = publication.tryClaim(ILINK_MESSAGE_HEADER + length, bufferClaim);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        iLinkMessage
            .wrapAndApplyHeader(buffer, offset, messageHeader)
            .connection(connectionId);

        offset += ILINK_MESSAGE_HEADER;

        // TODO: SOFH

        iLinkMessageHeader
            .wrap(buffer, offset)
            .blockLength(message.sbeBlockLength())
            .templateId(message.sbeTemplateId())
            .schemaId(message.sbeSchemaId())
            .version(message.sbeSchemaVersion());

        offset += iLinkMessageHeader.encodedLength();

        message.wrap(buffer, offset);

        return position;
    }
}
