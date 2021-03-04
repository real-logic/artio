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

import b3.entrypoint.fixp.sbe.*;
import io.aeron.ExclusivePublication;
import org.agrona.MutableDirectBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.fixp.AbstractFixPProxy;
import uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader;

import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

public class BinaryEntryPointProxy extends AbstractFixPProxy
{
    public static final int BINARY_ENTRYPOINT_HEADER_LENGTH = SOFH_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int BINARY_ENTRYPOINT_MESSAGE_HEADER = ARTIO_HEADER_LENGTH + BINARY_ENTRYPOINT_HEADER_LENGTH;

    private final MessageHeaderEncoder beMessageHeader = new MessageHeaderEncoder();
    private final NegotiateResponseEncoder negotiateResponse = new NegotiateResponseEncoder();
    private final NegotiateRejectEncoder negotiateReject = new NegotiateRejectEncoder();
    private final EstablishAckEncoder establishAck = new EstablishAckEncoder();
    private final EstablishRejectEncoder establishReject = new EstablishRejectEncoder();
    private final SequenceEncoder sequence = new SequenceEncoder();
    private final TerminateEncoder terminate = new TerminateEncoder();

    public BinaryEntryPointProxy(
        final long connectionId,
        final ExclusivePublication publication)
    {
        super(connectionId, publication);
    }

    public long sendSequence(final long uuid, final long nextSentSeqNo)
    {
        return 0;
    }

    public long sendNegotiateResponse(
        final long sessionID, final long sessionVerID, final long requestTimestamp, final long enteringFirm)
    {
        final NegotiateResponseEncoder negotiateResponse = this.negotiateResponse;

        final long position = claimMessage(NegotiateResponseEncoder.BLOCK_LENGTH, negotiateResponse, requestTimestamp);
        if (position < 0)
        {
            return position;
        }

        negotiateResponse
            .sessionID(sessionID)
            .sessionVerID(sessionVerID)
            .requestTimestamp().time(requestTimestamp);
        negotiateResponse
            .enteringFirm(enteringFirm);

        commit();

        return position;
    }

    public long sendNegotiateReject(
        final long sessionID,
        final long sessionVerID,
        final long requestTimestamp,
        final long enteringFirm,
        final NegotiationRejectCode negotiationRejectCode)
    {
        final NegotiateRejectEncoder negotiateReject = this.negotiateReject;

        final long position = claimMessage(NegotiateRejectEncoder.BLOCK_LENGTH, negotiateReject, requestTimestamp);
        if (position < 0)
        {
            return position;
        }

        negotiateReject
            .sessionID(sessionID)
            .sessionVerID(sessionVerID)
            .requestTimestamp().time(requestTimestamp);
        negotiateReject
            .enteringFirm(enteringFirm)
            .negotiationRejectCode(negotiationRejectCode);

        commit();

        return position;
    }

    public long sendEstablishAck(
        final long sessionID,
        final long sessionVerID,
        final long requestTimestamp,
        final long keepAliveInterval,
        final long nextSeqNo, final long lastIncomingSeqNo)
    {
        final EstablishAckEncoder establishAck = this.establishAck;

        final long position = claimMessage(EstablishAckEncoder.BLOCK_LENGTH, establishAck, requestTimestamp);
        if (position < 0)
        {
            return position;
        }

        establishAck
            .sessionID(sessionID)
            .sessionVerID(sessionVerID)
            .requestTimestamp().time(requestTimestamp);
        establishAck.keepAliveInterval().time(keepAliveInterval);
        establishAck
            .nextSeqNo(nextSeqNo)
            .lastIncomingSeqNo(lastIncomingSeqNo);

        commit();

        return position;
    }

    public long sendEstablishReject(
        final long sessionID,
        final long sessionVerID,
        final long requestTimestamp,
        final EstablishRejectCode establishmentRejectCode)
    {
        final EstablishRejectEncoder establishReject = this.establishReject;

        final long position = claimMessage(EstablishRejectEncoder.BLOCK_LENGTH, establishReject, requestTimestamp);
        if (position < 0)
        {
            return position;
        }

        establishReject
            .sessionID(sessionID)
            .sessionVerID(sessionVerID)
            .requestTimestamp().time(requestTimestamp);
        establishReject.establishmentRejectCode(establishmentRejectCode);

        commit();

        return position;
    }

    public long claimMessage(
        final int messageLength,
        final MessageEncoderFlyweight message,
        final long timestampInNs)
    {
        return claimMessage(
            messageLength,
            message,
            timestampInNs,
            BINARY_ENTRYPOINT_MESSAGE_HEADER,
            BINARY_ENTRYPOINT_HEADER_LENGTH,
            SimpleOpenFramingHeader.BINARY_ENTRYPOINT_TYPE);
    }

    protected int applyHeader(
        final MessageEncoderFlyweight message, final MutableDirectBuffer buffer, final int offset)
    {
        beMessageHeader
            .wrap(buffer, offset)
            .blockLength(message.sbeBlockLength())
            .templateId(message.sbeTemplateId())
            .schemaId(message.sbeSchemaId())
            .version(message.sbeSchemaVersion());

        return offset + beMessageHeader.encodedLength();
    }

    public long sendTerminate(
        final long sessionId,
        final long sessionVerId,
        final TerminationCode terminationCode,
        final long timestampInNs)
    {
        final TerminateEncoder terminate = this.terminate;

        final long position = claimMessage(TerminateEncoder.BLOCK_LENGTH, terminate, timestampInNs);
        if (position < 0)
        {
            return position;
        }

        terminate
            .sessionID(sessionId)
            .sessionVerID(sessionVerId)
            .terminationCode(terminationCode);

        commit();

        return position;
    }
}
