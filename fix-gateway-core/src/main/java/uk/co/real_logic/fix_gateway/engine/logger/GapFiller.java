/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.SequenceResetEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.ResendRequestDecoder;
import uk.co.real_logic.fix_gateway.decoder.SequenceResetDecoder;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.MessageStatus;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.SessionSubscription;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

public class GapFiller implements SessionHandler, Agent
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final int ENCODE_BUFFER_SIZE = 8 * 1024;

    private final AsciiBuffer decoderBuffer = new MutableAsciiBuffer();
    private final SessionSubscription sessionSubscription = new SessionSubscription(this);

    private final SequenceResetEncoder sequenceResetEncoder = new SequenceResetEncoder();
    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final MutableAsciiBuffer encodeBuffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final Subscription subscription;
    private final GatewayPublication publication;

    public GapFiller(final Subscription subscription, final GatewayPublication publication)
    {
        this.subscription = subscription;
        this.publication = publication;
        sequenceResetEncoder.gapFillFlag(true);
    }

    public int doWork() throws Exception
    {
        return subscription.poll(sessionSubscription, FRAGMENT_LIMIT);
    }

    public void onMessage(final DirectBuffer buffer,
                          final int offset,
                          final int length,
                          final int libraryId,
                          final long connectionId,
                          final long sessionId,
                          final int messageType,
                          final long timestamp)
    {
        if (messageType == ResendRequestDecoder.MESSAGE_TYPE)
        {
            decoderBuffer.wrap(buffer);
            resendRequest.decode(decoderBuffer, offset, length);

            final HeaderDecoder reqHeader = resendRequest.header();
            final HeaderEncoder respHeader = sequenceResetEncoder.header();
            respHeader.targetCompID(reqHeader.senderCompID(), reqHeader.senderCompIDLength());
            respHeader.senderCompID(reqHeader.targetCompID(), reqHeader.targetCompIDLength());
            if (reqHeader.hasSenderLocationID())
            {
                respHeader.targetLocationID(reqHeader.senderLocationID(), reqHeader.senderLocationIDLength());
            }
            if (reqHeader.hasSenderSubID())
            {
                respHeader.targetSubID(reqHeader.senderSubID(), reqHeader.senderSubIDLength());
            }
            if (reqHeader.hasTargetLocationID())
            {
                respHeader.senderLocationID(reqHeader.targetLocationID(), reqHeader.targetLocationIDLength());
            }
            if (reqHeader.hasTargetSubID())
            {
                respHeader.senderSubID(reqHeader.targetSubID(), reqHeader.targetSubIDLength());
            }
            respHeader.sendingTime(timestampEncoder.buffer(), timestampEncoder.encode(System.currentTimeMillis()));
            respHeader.msgSeqNum(resendRequest.beginSeqNo());
            sequenceResetEncoder.newSeqNo(resendRequest.endSeqNo());

            final int encodedLength = sequenceResetEncoder.encode(encodeBuffer, 0);
            publication.saveMessage(
                encodeBuffer, 0, encodedLength,
                libraryId, SequenceResetDecoder.MESSAGE_TYPE, sessionId, connectionId,
                MessageStatus.OK);
        }
    }

    public void onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
    }

    public String roleName()
    {
        return "GapFiller";
    }
}
