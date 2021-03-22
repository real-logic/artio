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
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.fixp.AbstractFixPProxy;
import uk.co.real_logic.artio.fixp.FixPContext;
import uk.co.real_logic.artio.fixp.FirstMessageRejectReason;
import uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader;

import java.nio.ByteBuffer;

import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.BINARY_ENTRYPOINT_TYPE;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

public class BinaryEntryPointProxy extends AbstractFixPProxy
{
    public static final int BINARY_ENTRYPOINT_HEADER_LENGTH = SOFH_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int BINARY_ENTRYPOINT_MESSAGE_HEADER = ARTIO_HEADER_LENGTH + BINARY_ENTRYPOINT_HEADER_LENGTH;
    private static final int NEGOTIATE_REJECT_LENGTH = BINARY_ENTRYPOINT_HEADER_LENGTH +
        NegotiateRejectEncoder.BLOCK_LENGTH;
    private static final int ESTABLISH_REJECT_LENGTH = BINARY_ENTRYPOINT_HEADER_LENGTH +
        EstablishRejectEncoder.BLOCK_LENGTH;

    private final MessageHeaderEncoder beMessageHeader = new MessageHeaderEncoder();
    private final NegotiateResponseEncoder negotiateResponse = new NegotiateResponseEncoder();
    private final NegotiateRejectEncoder negotiateReject = new NegotiateRejectEncoder();
    private final EstablishAckEncoder establishAck = new EstablishAckEncoder();
    private final EstablishRejectEncoder establishReject = new EstablishRejectEncoder();
    private final SequenceEncoder sequence = new SequenceEncoder();
    private final TerminateEncoder terminate = new TerminateEncoder();
    private final FinishedReceivingEncoder finishedReceiving = new FinishedReceivingEncoder();
    private final FinishedSendingEncoder finishedSending = new FinishedSendingEncoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer();

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
            BINARY_ENTRYPOINT_TYPE);
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

    public long sendFinishedReceiving(final long sessionID, final long sessionVerId, final long timestampInNs)
    {
        final FinishedReceivingEncoder finishedReceiving = this.finishedReceiving;

        final long position = claimMessage(FinishedReceivingEncoder.BLOCK_LENGTH, finishedReceiving, timestampInNs);
        if (position < 0)
        {
            return position;
        }

        finishedReceiving
            .sessionID(sessionID)
            .sessionVerID(sessionVerId);

        commit();

        return position;
    }

    public long sendFinishedSending(
        final long sessionId, final long sessionVerId, final long lastSeqNo, final long timestampInNs)
    {
        final FinishedSendingEncoder finishedSending = this.finishedSending;

        final long position = claimMessage(FinishedSendingEncoder.BLOCK_LENGTH, finishedSending, timestampInNs);
        if (position < 0)
        {
            return position;
        }

        finishedSending
            .sessionID(sessionId)
            .sessionVerID(sessionVerId)
            .lastSeqNo(lastSeqNo);

        commit();

        return position;
    }

    public ByteBuffer encodeReject(
        final FixPContext fixPContext, final FirstMessageRejectReason rejectReason)
    {
        final BinaryEntryPointContext identification = (BinaryEntryPointContext)fixPContext;

        final boolean isNegotiate;
        final NegotiationRejectCode negotiationRejectCode;
        final EstablishRejectCode establishRejectCode;
        switch (rejectReason)
        {
            case CREDENTIALS:
                isNegotiate = identification.fromNegotiate();
                negotiationRejectCode = NegotiationRejectCode.CREDENTIALS;
                establishRejectCode = EstablishRejectCode.CREDENTIALS;
                break;

            case NEGOTIATE_DUPLICATE_ID:
                isNegotiate = true;
                negotiationRejectCode = NegotiationRejectCode.DUPLICATE_ID;
                establishRejectCode = null;
                break;

            case NEGOTIATE_UNSPECIFIED:
                isNegotiate = true;
                negotiationRejectCode = NegotiationRejectCode.UNSPECIFIED;
                establishRejectCode = null;
                break;

            case ESTABLISH_UNNEGOTIATED:
                isNegotiate = false;
                negotiationRejectCode = null;
                establishRejectCode = EstablishRejectCode.UNNEGOTIATED;
                break;

            default:
                throw new IllegalArgumentException("Invalid reject reason: " + rejectReason);
        }

        final ByteBuffer byteBuffer;
        if (isNegotiate)
        {
            byteBuffer = ByteBuffer.allocate(NEGOTIATE_REJECT_LENGTH);
            buffer.wrap(byteBuffer);

            SimpleOpenFramingHeader.writeSofh(buffer, 0, NEGOTIATE_REJECT_LENGTH, BINARY_ENTRYPOINT_TYPE);
            negotiateReject
                .wrapAndApplyHeader(buffer, SOFH_LENGTH, beMessageHeader)
                .sessionID(identification.sessionID())
                .sessionVerID(identification.sessionVerID())
                .requestTimestamp().time(identification.requestTimestamp());
            negotiateReject
                .enteringFirm(identification.enteringFirm())
                .negotiationRejectCode(negotiationRejectCode);
        }
        else
        {
            byteBuffer = ByteBuffer.allocate(ESTABLISH_REJECT_LENGTH);
            buffer.wrap(byteBuffer);

            SimpleOpenFramingHeader.writeSofh(buffer, 0, ESTABLISH_REJECT_LENGTH, BINARY_ENTRYPOINT_TYPE);
            establishReject
                .wrapAndApplyHeader(buffer, SOFH_LENGTH, beMessageHeader)
                .sessionID(identification.sessionID())
                .sessionVerID(identification.sessionVerID())
                .requestTimestamp().time(identification.requestTimestamp());
            establishReject
                .establishmentRejectCode(establishRejectCode);
        }

        return byteBuffer;
    }
}
