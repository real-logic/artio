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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

public class SequenceNumbers implements Index
{
    public static final int NONE = -1;

    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final AsciiFlyweight asciiFlyweight = new AsciiFlyweight();
    private final Long2LongHashMap sessionIdToLastKnownSequenceNumber = new Long2LongHashMap(NONE);

    public SequenceNumbers()
    {
    }

    public int get(final long sessionId)
    {
        synchronized (sessionIdToLastKnownSequenceNumber)
        {
            return (int) sessionIdToLastKnownSequenceNumber.get(sessionId);
        }
    }

    public void onMessage(final long sessionId, final int sequenceNumber)
    {
        sessionIdToLastKnownSequenceNumber.put(sessionId, sequenceNumber);
    }

    @Override
    public void indexRecord(
        final DirectBuffer buffer, final int srcOffset, final int length, final int streamId, final int aeronSessionId)
    {
        int offset = srcOffset;
        frameHeaderDecoder.wrap(buffer, offset);
        if (frameHeaderDecoder.templateId() == FixMessageEncoder.TEMPLATE_ID)
        {
            final int actingBlockLength = frameHeaderDecoder.blockLength();
            offset += frameHeaderDecoder.encodedLength();
            messageFrame.wrap(buffer, offset, actingBlockLength, frameHeaderDecoder.version());

            offset += actingBlockLength + 2;

            asciiFlyweight.wrap(buffer);
            fixHeader.decode(asciiFlyweight, offset, messageFrame.bodyLength());

            // TODO: remove synchronisation and move offheap
            synchronized (sessionIdToLastKnownSequenceNumber)
            {
                sessionIdToLastKnownSequenceNumber.put(messageFrame.session(), fixHeader.msgSeqNum());
            }
        }
    }

    public int query(final long sessionId)
    {
        synchronized (sessionIdToLastKnownSequenceNumber)
        {
            return get(sessionId);
        }
    }

    @Override
    public void close()
    {

    }

}
