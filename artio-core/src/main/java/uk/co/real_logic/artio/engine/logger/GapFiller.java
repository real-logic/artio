/*
 * Copyright 2015-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.logger;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.decoder.SequenceResetDecoder;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.ProtocolHandler;
import uk.co.real_logic.artio.protocol.ProtocolSubscription;
import uk.co.real_logic.artio.replication.ClusterableSubscription;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

public class GapFiller implements ProtocolHandler, Agent
{
    private static final int FRAGMENT_LIMIT = 10;

    private final AsciiBuffer decoderBuffer = new MutableAsciiBuffer();
    private final ProtocolSubscription protocolSubscription = ProtocolSubscription.of(this);

    private final GapFillEncoder encoder = new GapFillEncoder();

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final ClusterableSubscription subscription;
    private final GatewayPublication publication;
    private final String agentNamePrefix;

    public GapFiller(
        final ClusterableSubscription subscription,
        final GatewayPublication publication,
        final String agentNamePrefix)
    {
        this.subscription = subscription;
        this.publication = publication;
        this.agentNamePrefix = agentNamePrefix;
    }

    public int doWork() throws Exception
    {
        return subscription.poll(protocolSubscription, FRAGMENT_LIMIT);
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final int messageType,
        final long timestamp,
        final MessageStatus status,
        final long position)
    {
        if (messageType == ResendRequestDecoder.MESSAGE_TYPE && status == MessageStatus.OK)
        {
            decoderBuffer.wrap(buffer);
            resendRequest.decode(decoderBuffer, offset, length);

            final HeaderDecoder reqHeader = resendRequest.header();
            final int beginSeqNo = resendRequest.beginSeqNo();
            final int endSeqNo = resendRequest.endSeqNo();

            final long result = encoder.encode(reqHeader, beginSeqNo, endSeqNo);
            final int encodedLength = Encoder.length(result);
            final int encodedOffset = Encoder.offset(result);
            final long sentPosition = publication.saveMessage(
                encoder.buffer(), encodedOffset, encodedLength,
                libraryId, SequenceResetDecoder.MESSAGE_TYPE, sessionId, sequenceIndex, connectionId,
                MessageStatus.OK);

            if (Pressure.isBackPressured(sentPosition))
            {
                return Action.ABORT;
            }
        }

        return Action.CONTINUE;
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        return Action.CONTINUE;
    }

    public String roleName()
    {
        return agentNamePrefix + "GapFiller";
    }
}
