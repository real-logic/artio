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
package uk.co.real_logic.artio.ilink;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.engine.logger.FixPSequenceNumberHandler;
import uk.co.real_logic.artio.fixp.AbstractFixPOffsets;
import uk.co.real_logic.artio.fixp.AbstractFixPSequenceExtractor;
import uk.co.real_logic.artio.messages.FixPMessageDecoder;
import uk.co.real_logic.artio.messages.FollowerSessionRequestDecoder;

import static uk.co.real_logic.artio.fixp.AbstractFixPParser.BOOLEAN_FLAG_TRUE;
import static uk.co.real_logic.artio.fixp.AbstractFixPParser.FIXP_MESSAGE_HEADER_LENGTH;

class ILink3SequenceExtractor extends AbstractFixPSequenceExtractor
{
    private final ILink3Offsets offsets;
    private final ILink3Parser parser;

    ILink3SequenceExtractor(
        final FixPSequenceNumberHandler handler,
        final ILink3Offsets offsets,
        final ILink3Parser parser)
    {
        super(handler);
        this.offsets = offsets;
        this.parser = parser;
    }

    public void onMessage(
        final FixPMessageDecoder fixPMessage,
        final DirectBuffer buffer,
        final int headerOffset,
        final int totalLength,
        final long endPosition,
        final int aeronSessionId,
        final long timestamp)
    {
        final long uuid = fixPMessage.sessionId();
        final int templateId = parser.templateId(buffer, headerOffset);
        final int messageOffset = headerOffset + FIXP_MESSAGE_HEADER_LENGTH;
        final boolean possRetrans = offsets.possRetrans(templateId, buffer, messageOffset) == BOOLEAN_FLAG_TRUE;

        final int seqNum = offsets.seqNum(templateId, buffer, messageOffset);
        if (seqNum != AbstractFixPOffsets.MISSING_OFFSET)
        {
            handler.onSequenceNumber(seqNum, uuid, totalLength, endPosition, aeronSessionId, possRetrans, timestamp,
                false);
        }
    }

    public void onRedactSequenceUpdate(final long sessionId, final int newSequenceNumber)
    {
    }

    public void onFollowerSessionRequest(
        final FollowerSessionRequestDecoder followerSessionRequest,
        final long endPosition,
        final int totalLength,
        final int aeronSessionId)
    {
    }
}
