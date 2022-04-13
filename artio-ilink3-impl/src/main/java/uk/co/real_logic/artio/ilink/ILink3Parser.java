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
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.fixp.AbstractFixPParser;
import uk.co.real_logic.artio.fixp.FixPContext;
import uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader;
import uk.co.real_logic.artio.library.InternalILink3Connection;

import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.FIXP_SESSION;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

public class ILink3Parser extends AbstractFixPParser
{
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final NegotiationResponse501Decoder negotiationResponse = new NegotiationResponse501Decoder();
    private final NegotiationReject502Decoder negotiationReject = new NegotiationReject502Decoder();
    private final EstablishmentAck504Decoder establishmentAck = new EstablishmentAck504Decoder();
    private final EstablishmentReject505Decoder establishmentReject = new EstablishmentReject505Decoder();
    private final Terminate507Decoder terminate = new Terminate507Decoder();
    private final Sequence506Decoder sequence = new Sequence506Decoder();
    private final NotApplied513Decoder notApplied = new NotApplied513Decoder();
    private final RetransmitReject510Decoder retransmitReject = new RetransmitReject510Decoder();
    private final Retransmission509Decoder retransmission = new Retransmission509Decoder();

    private final Consumer<StringBuilder> negotiationResponseAppendTo = negotiationResponse::appendTo;
    private final Consumer<StringBuilder> negotiationRejectAppendTo = negotiationReject::appendTo;
    private final Consumer<StringBuilder> establishmentAckAppendTo = establishmentAck::appendTo;
    private final Consumer<StringBuilder> establishmentRejectAppendTo = establishmentReject::appendTo;
    private final Consumer<StringBuilder> terminateAppendTo = terminate::appendTo;
    private final Consumer<StringBuilder> sequenceAppendTo = sequence::appendTo;
    private final Consumer<StringBuilder> notAppliedAppendTo = notApplied::appendTo;
    private final Consumer<StringBuilder> retransmitRejectAppendTo = retransmitReject::appendTo;
    private final Consumer<StringBuilder> retransmissionAppendTo = retransmission::appendTo;

    private final InternalILink3Connection handler;

    public ILink3Parser(final ILink3Connection handler)
    {
        this.handler = (InternalILink3Connection)handler;
    }

    public int templateId(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        return header.templateId();
    }

    public int blockLength(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        return header.blockLength();
    }

    public int version(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        return header.version();
    }

    public FixPContext lookupContext(
        final DirectBuffer messageBuffer,
        final int messageOffset,
        final int messageLength)
    {
        return Ilink3Protocol.unsupported();
    }

    public long sessionId(final DirectBuffer buffer, final int offset)
    {
        return Ilink3Protocol.unsupported();
    }

    public Action onMessage(final DirectBuffer buffer, final int start)
    {
        int offset = start + SOFH_LENGTH;

        header.wrap(buffer, offset);
        final int templateId = header.templateId();
        final int blockLength = header.blockLength();
        final int version = header.version();

        offset += MessageHeaderEncoder.ENCODED_LENGTH;

        switch (templateId)
        {
            case NegotiationResponse501Decoder.TEMPLATE_ID:
            {
                return onNegotiationResponse(buffer, offset, blockLength, version);
            }

            case NegotiationReject502Decoder.TEMPLATE_ID:
            {
                return onNegotiationReject(buffer, offset, blockLength, version);
            }

            case EstablishmentAck504Decoder.TEMPLATE_ID:
            {
                return onEstablishmentAck(buffer, offset, blockLength, version);
            }

            case EstablishmentReject505Decoder.TEMPLATE_ID:
            {
                return onEstablishmentReject(buffer, offset, blockLength, version);
            }

            case Terminate507Decoder.TEMPLATE_ID:
            {
                return onTerminate(buffer, offset, blockLength, version);
            }

            case Sequence506Decoder.TEMPLATE_ID:
            {
                return onSequence(buffer, offset, blockLength, version);
            }

            case NotApplied513Decoder.TEMPLATE_ID:
            {
                return onNotApplied(buffer, offset, blockLength, version);
            }

            case RetransmitReject510Decoder.TEMPLATE_ID:
            {
                return onRetransmitReject(buffer, offset, blockLength, version);
            }

            case Retransmission509Decoder.TEMPLATE_ID:
            {
                return onRetransmission(buffer, offset, blockLength, version);
            }

            default:
            {
                final int sofhMessageSize = SimpleOpenFramingHeader.readSofhMessageSize(buffer, start);
                return handler.onMessage(buffer, offset, templateId, blockLength, version, sofhMessageSize);
            }
        }
    }

    private Action onRetransmission(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        retransmission.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", retransmissionAppendTo);
        return handler.onRetransmission(
            retransmission.uUID(),
            retransmission.lastUUID(),
            retransmission.requestTimestamp(),
            retransmission.fromSeqNo(),
            retransmission.msgCount());
//        retransmitReject.splitMsg()
    }

    private Action onRetransmitReject(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        retransmitReject.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", retransmitRejectAppendTo);
        return handler.onRetransmitReject(
            retransmitReject.reason(),
            retransmitReject.uUID(),
            retransmitReject.lastUUID(),
            retransmitReject.requestTimestamp(),
            retransmitReject.errorCodes());
//        retransmitReject.splitMsg()
    }

    private Action onNegotiationResponse(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        negotiationResponse.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", negotiationResponseAppendTo);
        return handler.onNegotiationResponse(
            negotiationResponse.uUID(),
            negotiationResponse.requestTimestamp(),
            negotiationResponse.secretKeySecureIDExpiration(),
            // negotiationResponse.faultToleranceIndicator()
            // negotiationResponse.splitMsg()
            negotiationResponse.previousSeqNo(),
            negotiationResponse.previousUUID());
    }

    private Action onNegotiationReject(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        negotiationReject.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", negotiationRejectAppendTo);
        return handler.onNegotiationReject(
            negotiationReject.reason(),
            negotiationReject.uUID(),
            negotiationReject.requestTimestamp(),
            negotiationReject.errorCodes());
            // negotiationResponse.faultToleranceIndicator()
            // negotiationResponse.splitMsg());
    }

    private Action onEstablishmentAck(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        establishmentAck.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", establishmentAckAppendTo);
        return handler.onEstablishmentAck(
            establishmentAck.uUID(),
            establishmentAck.requestTimestamp(),
            establishmentAck.nextSeqNo(),
            establishmentAck.previousSeqNo(),
            establishmentAck.previousUUID(),
            establishmentAck.keepAliveInterval(),
            establishmentAck.secretKeySecureIDExpiration());
            // establishmentAck.faultToleranceIndicator()
            // establishmentAck.splitMsg()
    }

    private Action onEstablishmentReject(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        establishmentReject.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", establishmentRejectAppendTo);
        return handler.onEstablishmentReject(
            establishmentReject.reason(),
            establishmentReject.uUID(),
            establishmentReject.requestTimestamp(),
            establishmentReject.nextSeqNo(),
            establishmentReject.errorCodes());
        // establishmentReject.faultToleranceIndicator()
        // establishmentReject.splitMsg()
    }

    private Action onTerminate(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        terminate.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", terminateAppendTo);
        return handler.onTerminate(
            terminate.reason(),
            terminate.uUID(),
            terminate.requestTimestamp(),
            terminate.errorCodes());
            // terminate.splitMsg()
    }

    private Action onSequence(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        sequence.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", sequenceAppendTo);
        return handler.onSequence(
            sequence.uUID(),
            sequence.nextSeqNo(),
            sequence.faultToleranceIndicator(),
            sequence.keepAliveIntervalLapsed());
    }

    private Action onNotApplied(final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        notApplied.wrap(buffer, offset, blockLength, version);
        DebugLogger.logSbeDecoder(FIXP_SESSION, "> ", notAppliedAppendTo);
        return handler.onNotApplied(
            notApplied.uUID(),
            notApplied.fromSeqNo(),
            notApplied.msgCount());
//            notApplied.splitMsg()
    }

    public int retransmissionTemplateId()
    {
        return Retransmission509Decoder.TEMPLATE_ID;
    }

    public boolean isRetransmittedMessage(final DirectBuffer buffer, final int offset)
    {
        return true;
    }
}
