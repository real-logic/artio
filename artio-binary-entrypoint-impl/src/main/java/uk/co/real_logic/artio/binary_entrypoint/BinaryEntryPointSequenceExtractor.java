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

import b3.entrypoint.fixp.sbe.MessageHeaderDecoder;
import b3.entrypoint.fixp.sbe.NegotiateResponseDecoder;
import b3.entrypoint.fixp.sbe.SequenceDecoder;
import b3.entrypoint.fixp.sbe.SimpleNewOrderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.artio.engine.logger.FixPSequenceNumberHandler;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.fixp.AbstractFixPSequenceExtractor;
import uk.co.real_logic.artio.messages.FixPMessageDecoder;

import java.util.function.LongFunction;

import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;

class BinaryEntryPointSequenceExtractor extends AbstractFixPSequenceExtractor
{
    private static final int LOWEST_APP_TEMPLATE_ID = SimpleNewOrderDecoder.TEMPLATE_ID;

    private final Long2ObjectHashMap<Info> sessionIdToInfo = new Long2ObjectHashMap<>();
    private final LongFunction<Info> onNewConnectionFunc = this::onNewConnection;
    private final MessageHeaderDecoder beHeader = new MessageHeaderDecoder();
    private final SequenceNumberIndexReader sequenceNumberReader;
    private final SequenceDecoder sequence = new SequenceDecoder();

    BinaryEntryPointSequenceExtractor(
        final FixPSequenceNumberHandler handler,
        final SequenceNumberIndexReader sequenceNumberReader)
    {
        super(handler);
        this.sequenceNumberReader = sequenceNumberReader;
    }

    public void onMessage(
        final FixPMessageDecoder fixPMessage,
        final DirectBuffer buffer,
        final int headerOffset,
        final int totalLength,
        final long endPosition,
        final int aeronSessionId)
    {
        final int templateId = beHeader.wrap(buffer, headerOffset).templateId();
        final int msgOffset = headerOffset + MessageHeaderDecoder.ENCODED_LENGTH;
        final long sessionId = fixPMessage.sessionId();

        if (templateId == NegotiateResponseDecoder.TEMPLATE_ID)
        {
            // Success sequence number reset
            final Info info = lookupInfo(sessionId);
            info.lastSequenceNumber = 0;
            onSequenceNumber(totalLength, endPosition, aeronSessionId, info);
        }
        else if (templateId == SequenceDecoder.TEMPLATE_ID)
        {
            sequence.wrap(buffer, msgOffset, beHeader.blockLength(), beHeader.version());
            final Info info = lookupInfo(sessionId);
            info.lastSequenceNumber = (int)(sequence.nextSeqNo() - 1);
        }
        else if (templateId >= LOWEST_APP_TEMPLATE_ID)
        {
            final Info info = lookupInfo(sessionId);
            info.lastSequenceNumber++;
            onSequenceNumber(totalLength, endPosition, aeronSessionId, info);
        }
    }

    public void onRedactSequenceUpdate(final long sessionId, final int newSequenceNumber)
    {
        final Info info = lookupInfo(sessionId);
        info.lastSequenceNumber = newSequenceNumber;
    }

    private Info lookupInfo(final long sessionId)
    {
        return sessionIdToInfo.computeIfAbsent(sessionId, onNewConnectionFunc);
    }

    private void onSequenceNumber(
        final int totalLength, final long endPosition, final int aeronSessionId, final Info info)
    {
        handler.onSequenceNumber(
            info.lastSequenceNumber, info.sessionId, totalLength, endPosition, aeronSessionId, false);
    }

    private Info onNewConnection(final long sessionId)
    {
        int lastSequenceNumber = sequenceNumberReader.lastKnownSequenceNumber(sessionId);
        if (lastSequenceNumber == UNK_SESSION)
        {
            lastSequenceNumber = 0;
        }
        return new Info(sessionId, lastSequenceNumber);
    }

    static class Info
    {
        private final long sessionId;

        private int lastSequenceNumber;

        Info(final long sessionId, final int lastSequenceNumber)
        {
            this.sessionId = sessionId;
            this.lastSequenceNumber = lastSequenceNumber;
        }
    }
}
