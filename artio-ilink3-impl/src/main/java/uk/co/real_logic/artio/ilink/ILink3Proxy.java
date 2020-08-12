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

import iLinkBinary.*;
import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.messages.ILinkMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.ILINK_SESSION;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.SOFH_LENGTH;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.writeSofh;

public class ILink3Proxy extends AbstractILink3Proxy
{
    public static final int ILINK_HEADER_LENGTH = SOFH_LENGTH + iLinkBinary.MessageHeaderEncoder.ENCODED_LENGTH;

    private static final int ARTIO_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + ILinkMessageEncoder.BLOCK_LENGTH;
    private static final int ILINK_MESSAGE_HEADER = ARTIO_HEADER_LENGTH + ILINK_HEADER_LENGTH;

    private static final UnsafeBuffer NO_BUFFER = new UnsafeBuffer(new byte[0]);

    private static final int NEGOTIATE_LENGTH =
        Negotiate500Encoder.BLOCK_LENGTH + Negotiate500Encoder.credentialsHeaderLength();
    private static final int ESTABLISH_LENGTH =
        Establish503Encoder.BLOCK_LENGTH + Establish503Encoder.credentialsHeaderLength();

    private final ILinkMessageEncoder iLinkMessage = new ILinkMessageEncoder();
    private final BufferClaim bufferClaim = new BufferClaim();

    private long connectionId;
    private final ExclusivePublication publication;
    private final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
    private final iLinkBinary.MessageHeaderEncoder iLinkMessageHeader = new iLinkBinary.MessageHeaderEncoder();

    private final Negotiate500Encoder negotiate = new Negotiate500Encoder();
    private final Establish503Encoder establish = new Establish503Encoder();
    private final Terminate507Encoder terminate = new Terminate507Encoder();
    private final Sequence506Encoder sequence = new Sequence506Encoder();
    private final RetransmitRequest508Encoder retransmitRequest = new RetransmitRequest508Encoder();

    private final Consumer<StringBuilder> negotiateAppendTo = negotiate::appendTo;
    private final Consumer<StringBuilder> establishAppendTo = establish::appendTo;
    private final Consumer<StringBuilder> terminateAppendTo = terminate::appendTo;
    private final Consumer<StringBuilder> sequenceAppendTo = sequence::appendTo;
    private final Consumer<StringBuilder> retransmitRequestAppendTo = retransmitRequest::appendTo;

    public ILink3Proxy(final long connectionId, final ExclusivePublication publication)
    {
        this.connectionId = connectionId;
        this.publication = publication;
    }

    public void connectionId(final long connectionId)
    {
        this.connectionId = connectionId;
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

        final long position = claimILinkMessage(NEGOTIATE_LENGTH, negotiate);
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
            .firm(firmId)
            .putCredentials(NO_BUFFER, 0, 0);

        DebugLogger.logSbeDecoder(ILINK_SESSION, "< ", negotiateAppendTo);

        commit();

        return position;
    }

    public long sendEstablish(
        final byte[] hMACSignature,
        final String accessKeyId,
        final String tradingSystemName,
        final String tradingSystemVendor,
        final String tradingSystemVersion,
        final long uuid,
        final long requestTimestamp,
        final long nextSentSeqNo,
        final String sessionId,
        final String firmId,
        final int keepAliveInterval)
    {
        final Establish503Encoder establish = this.establish;

        final long position = claimILinkMessage(ESTABLISH_LENGTH, establish);
        if (position < 0)
        {
            return position;
        }

        establish
            .putHMACSignature(hMACSignature, 0)
            .accessKeyID(accessKeyId)
            .tradingSystemName(tradingSystemName)
            .tradingSystemVendor(tradingSystemVendor)
            .tradingSystemVersion(tradingSystemVersion)
            .uUID(uuid)
            .requestTimestamp(requestTimestamp)
            .nextSeqNo(nextSentSeqNo)
            .session(sessionId)
            .firm(firmId)
            .keepAliveInterval(keepAliveInterval)
            .putCredentials(NO_BUFFER, 0, 0);

        DebugLogger.logSbeDecoder(ILINK_SESSION, "< ", establishAppendTo);

        commit();

        return position;
    }

    public long sendTerminate(final String reason, final long uuid, final long requestTimestamp, final int errorCodes)
    {
        final Terminate507Encoder terminate = this.terminate;

        final long position = claimILinkMessage(Terminate507Encoder.BLOCK_LENGTH, terminate);
        if (position < 0)
        {
            return position;
        }

        terminate
            .reason(reason)
            .uUID(uuid)
            .requestTimestamp(requestTimestamp)
            .errorCodes(errorCodes)
            .splitMsg(SplitMsg.NULL_VAL);

        DebugLogger.logSbeDecoder(ILINK_SESSION, "< ", terminateAppendTo);

        commit();

        return position;
    }

    public long sendSequence(
        final long uuid, final long nextSentSeqNo)
    {
        return sendSequence(uuid, nextSentSeqNo, FTI.Primary, KeepAliveLapsed.NotLapsed);
    }

    public long sendSequence(
        final long uuid, final long nextSentSeqNo, final FTI fti, final KeepAliveLapsed keepAliveLapsed)
    {
        final Sequence506Encoder sequence = this.sequence;

        final long position = claimILinkMessage(Sequence506Encoder.BLOCK_LENGTH, sequence);
        if (position < 0)
        {
            return position;
        }

        sequence
            .uUID(uuid)
            .nextSeqNo(nextSentSeqNo)
            .faultToleranceIndicator(fti)
            .keepAliveIntervalLapsed(keepAliveLapsed);

        DebugLogger.logSbeDecoder(ILINK_SESSION, "< ", sequenceAppendTo);

        commit();

        return position;
    }

    public long sendRetransmitRequest(
        final long uuid, final long lastUuid, final long requestTimestamp, final long fromSeqNo, final int msgCount)
    {
        final RetransmitRequest508Encoder retransmitRequest = this.retransmitRequest;

        final long position = claimILinkMessage(RetransmitRequest508Encoder.BLOCK_LENGTH, retransmitRequest);
        if (position < 0)
        {
            return position;
        }

        retransmitRequest
            .uUID(uuid)
            .lastUUID(lastUuid)
            .requestTimestamp(requestTimestamp)
            .fromSeqNo(fromSeqNo)
            .msgCount(msgCount);

        DebugLogger.logSbeDecoder(ILINK_SESSION, "< ", retransmitRequestAppendTo);

        commit();

        return position;
    }

    public long claimILinkMessage(
        final int messageLength,
        final MessageEncoderFlyweight message)
    {
        final BufferClaim bufferClaim = this.bufferClaim;
        final long position = publication.tryClaim(ILINK_MESSAGE_HEADER + messageLength, bufferClaim);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        iLinkMessage
            .wrapAndApplyHeader(buffer, offset, messageHeader)
            .connection(connectionId);

        offset += ARTIO_HEADER_LENGTH;

        writeSofh(buffer, offset, ILINK_HEADER_LENGTH + messageLength);
        offset += SOFH_LENGTH;

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

    public void commit()
    {
        bufferClaim.commit();
    }

    public void abort()
    {
        bufferClaim.abort();
    }
}
