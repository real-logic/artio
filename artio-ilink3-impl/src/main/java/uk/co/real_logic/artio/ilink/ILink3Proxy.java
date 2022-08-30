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
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.fixp.*;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.FIXP_SESSION;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

public class ILink3Proxy extends AbstractFixPProxy
{
    public static final int ILINK_HEADER_LENGTH = SOFH_LENGTH + iLinkBinary.MessageHeaderEncoder.ENCODED_LENGTH;

    private static final int ILINK_MESSAGE_HEADER = ARTIO_HEADER_LENGTH + ILINK_HEADER_LENGTH;

    private static final UnsafeBuffer NO_BUFFER = new UnsafeBuffer(new byte[0]);

    private static final int NEGOTIATE_LENGTH =
        Negotiate500Encoder.BLOCK_LENGTH + Negotiate500Encoder.credentialsHeaderLength();
    private static final int ESTABLISH_LENGTH =
        Establish503Encoder.BLOCK_LENGTH + Establish503Encoder.credentialsHeaderLength();

    private final iLinkBinary.MessageHeaderEncoder iLinkMessageHeader = new iLinkBinary.MessageHeaderEncoder();
    private final iLinkBinary.MessageHeaderDecoder iLinkMessageHeaderDecoder = new iLinkBinary.MessageHeaderDecoder();

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

    private final EpochNanoClock epochNanoClock;

    public ILink3Proxy(
        final Ilink3Protocol protocol,
        final long connectionId,
        final ExclusivePublication publication,
        final FixPMessageDissector dissector,
        final EpochNanoClock epochNanoClock)
    {
        super(protocol, dissector, connectionId, publication);
        this.connectionId = connectionId;
        this.epochNanoClock = epochNanoClock;
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

        final long position = claimMessage(NEGOTIATE_LENGTH, negotiate, requestTimestamp);
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

        DebugLogger.logSbeDecoder(FIXP_SESSION, "< ", negotiateAppendTo);

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

        final long position = claimMessage(ESTABLISH_LENGTH, establish, requestTimestamp);
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

        DebugLogger.logSbeDecoder(FIXP_SESSION, "< ", establishAppendTo);

        commit();

        return position;
    }

    public long sendTerminate(final String reason, final long uuid, final long requestTimestamp, final int errorCodes)
    {
        final Terminate507Encoder terminate = this.terminate;

        final long position = claimMessage(Terminate507Encoder.BLOCK_LENGTH, terminate, requestTimestamp);
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

        DebugLogger.logSbeDecoder(FIXP_SESSION, "< ", terminateAppendTo);

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

        final long position = claimMessage(Sequence506Encoder.BLOCK_LENGTH, sequence, timestamp());
        if (position < 0)
        {
            return position;
        }

        sequence
            .uUID(uuid)
            .nextSeqNo(nextSentSeqNo)
            .faultToleranceIndicator(fti)
            .keepAliveIntervalLapsed(keepAliveLapsed);

        DebugLogger.logSbeDecoder(FIXP_SESSION, "< ", sequenceAppendTo);

        commit();

        return position;
    }

    private long timestamp()
    {
        return epochNanoClock.nanoTime();
    }

    public long sendRetransmitRequest(
        final long uuid, final long lastUuid, final long requestTimestamp, final long fromSeqNo, final int msgCount)
    {
        final RetransmitRequest508Encoder retransmitRequest = this.retransmitRequest;

        final long position = claimMessage(
            RetransmitRequest508Encoder.BLOCK_LENGTH, retransmitRequest, requestTimestamp);
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

        DebugLogger.logSbeDecoder(FIXP_SESSION, "< ", retransmitRequestAppendTo);

        commit();

        return position;
    }

    public long claimMessage(
        final int messageLength,
        final MessageEncoderFlyweight message,
        final long timestamp)
    {
        return claimMessage(
            messageLength,
            message,
            timestamp,
            ILINK_MESSAGE_HEADER,
            ILINK_HEADER_LENGTH,
            SimpleOpenFramingHeader.CME_ENCODING_TYPE);
    }

    protected int applyHeader(
        final MessageEncoderFlyweight message, final MutableDirectBuffer buffer, final int offset)
    {
        iLinkMessageHeader
            .wrap(buffer, offset)
            .blockLength(message.sbeBlockLength())
            .templateId(message.sbeTemplateId())
            .schemaId(message.sbeSchemaId())
            .version(message.sbeSchemaVersion());

        return offset + iLinkMessageHeader.encodedLength();
    }

    public ByteBuffer encodeReject(
        final FixPContext identification, final FixPFirstMessageResponse rejectReason, final Enum<?> rejectCode)
    {
        return Ilink3Protocol.unsupported();
    }

    public byte[] encodeFirstMessage(final FixPContext context)
    {
        return Ilink3Protocol.unsupported();
    }
}
