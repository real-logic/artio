/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.engine.ReplayerCommandQueue;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.ProtocolHandler;
import uk.co.real_logic.artio.protocol.ProtocolSubscription;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.dictionary.SessionConstants.RESEND_REQUEST_MESSAGE_TYPE;
import static uk.co.real_logic.artio.dictionary.SessionConstants.SEQUENCE_RESET_MESSAGE_TYPE;

public class GapFiller implements ProtocolHandler, Agent
{
    private static final int FRAGMENT_LIMIT = 10;

    private final AsciiBuffer decoderBuffer = new MutableAsciiBuffer();
    private final ReplayerCommandQueue replayerCommandQueue;
    private final FixSessionCodecsFactory fixSessionCodecsFactory;
    private final ControlledFragmentHandler protocolSubscription;

    private final Subscription inboundSubscription;
    private final GatewayPublication publication;
    private final String agentNamePrefix;
    private final SenderSequenceNumbers senderSequenceNumbers;

    public GapFiller(
        final Subscription inboundSubscription,
        final GatewayPublication publication,
        final String agentNamePrefix,
        final SenderSequenceNumbers senderSequenceNumbers,
        final ReplayerCommandQueue replayerCommandQueue,
        final FixSessionCodecsFactory fixSessionCodecsFactory)
    {
        this.inboundSubscription = inboundSubscription;
        this.publication = publication;
        this.agentNamePrefix = agentNamePrefix;
        this.senderSequenceNumbers = senderSequenceNumbers;
        this.replayerCommandQueue = replayerCommandQueue;
        this.fixSessionCodecsFactory = fixSessionCodecsFactory;
        this.protocolSubscription = ProtocolSubscription.of(this, fixSessionCodecsFactory);
    }

    public int doWork()
    {
        return replayerCommandQueue.poll() + inboundSubscription.controlledPoll(protocolSubscription, FRAGMENT_LIMIT);
    }

    public String roleName()
    {
        return agentNamePrefix + "GapFiller";
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final long messageType,
        final long timestamp,
        final MessageStatus status,
        final int sequenceNumber,
        final long position,
        final int metaDataLength)
    {
        if (messageType == RESEND_REQUEST_MESSAGE_TYPE && status == MessageStatus.OK)
        {
            decoderBuffer.wrap(buffer);
            final FixReplayerCodecs fixReplayerCodecs = fixSessionCodecsFactory.get(sessionId);
            final AbstractResendRequestDecoder resendRequest = fixReplayerCodecs.resendRequest();
            final GapFillEncoder encoder = fixReplayerCodecs.gapFillEncoder();

            resendRequest.decode(decoderBuffer, offset, length);

            final SessionHeaderDecoder reqHeader = resendRequest.header();
            final int beginSeqNo = resendRequest.beginSeqNo();
            final int endSeqNo = resendRequest.endSeqNo();
            final int lastSentSeqNo = newSeqNo(connectionId);

            // If the request was for an infinite replay then reply with the next expected sequence number
            final int newSeqNo = endSeqNo == 0 ? lastSentSeqNo : endSeqNo;
            final int gapFillMsgSeqNum = beginSeqNo;

            encoder.setupMessage(reqHeader);
            final long result = encoder.encode(gapFillMsgSeqNum, newSeqNo);
            final int encodedLength = Encoder.length(result);
            final int encodedOffset = Encoder.offset(result);
            final long sentPosition = publication.saveMessage(
                encoder.buffer(), encodedOffset, encodedLength,
                libraryId, SEQUENCE_RESET_MESSAGE_TYPE, sessionId, sequenceIndex, connectionId,
                MessageStatus.OK, gapFillMsgSeqNum);

            if (Pressure.isBackPressured(sentPosition))
            {
                return Action.ABORT;
            }
        }

        return Action.CONTINUE;
    }

    public Action onILinkMessage(final long connectionId, final DirectBuffer buffer, final int offset)
    {
        return CONTINUE;
    }

    private int newSeqNo(final long connectionId)
    {
        return senderSequenceNumbers.lastSentSequenceNumber(connectionId) + 1;
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        return Action.CONTINUE;
    }
}
