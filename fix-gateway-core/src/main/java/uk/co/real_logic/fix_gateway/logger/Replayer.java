/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.decoder.ResendRequestDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.session.SessionHandler;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

public class Replayer implements SessionHandler
{
    public static final int SIZE_OF_LENGTH_FIELD = 2;

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final AsciiFlyweight asciiFlyweight = new AsciiFlyweight();
    private final MutableAsciiFlyweight mutableAsciiFlyweight = new MutableAsciiFlyweight();
    private final QueryService queryService;
    private final Publication publication;

    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    public Replayer(final QueryService queryService, final Publication publication)
    {
        this.queryService = queryService;
        this.publication = publication;
    }

    public void onMessage(final DirectBuffer srcBuffer,
                          final int srcOffset,
                          final int length,
                          final long connectionId,
                          final long sessionId,
                          final int messageType)
    {
        if (messageType == ResendRequestDecoder.MESSAGE_TYPE)
        {
            asciiFlyweight.wrap(srcBuffer);
            resendRequest.decode(asciiFlyweight, srcOffset, length);

            final BufferClaim claim = queryMessages(sessionId);

            if (claim != null)
            {
                final MutableDirectBuffer claimBuffer = claim.buffer();
                final int claimLength = claim.length();
                int claimOffset = claim.offset();
                final int end = claimOffset + claimLength;

                while (claimOffset < end)
                {
                    messageFrameHeader.wrap(claimBuffer, claimOffset, claimLength);
                    claimOffset += messageFrameHeader.size();
                    final int actingBlockLength = messageFrameHeader.blockLength();
                    messageFrame.wrap(claimBuffer, claimOffset, actingBlockLength, messageFrameHeader.version());
                    claimOffset += SIZE_OF_LENGTH_FIELD + actingBlockLength;
                    putPossibleDuplicate(claimBuffer, claimOffset);
                    claimOffset += messageFrame.bodyLength();
                }

                claim.commit();
            }
        }
    }

    private void putPossibleDuplicate(final MutableDirectBuffer claimBuffer, final int claimOffset)
    {
        final int possDupOffset = claimOffset + messageFrame.possDupOffset();
        mutableAsciiFlyweight.wrap(claimBuffer);
        mutableAsciiFlyweight.putChar(possDupOffset, 'Y');
    }

    private BufferClaim queryMessages(final long sessionId)
    {
        final int beginSeqNo = resendRequest.beginSeqNo();
        final int endSeqNo = resendRequest.endSeqNo();
        if (endSeqNo < beginSeqNo)
        {
            // TODO: log error
            return null;
        }
        return queryService.query(publication, sessionId, beginSeqNo, endSeqNo);
    }

}
