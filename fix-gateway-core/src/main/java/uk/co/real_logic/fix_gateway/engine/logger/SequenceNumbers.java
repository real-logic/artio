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
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.Hashing;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.RecordBuffer;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static uk.co.real_logic.agrona.concurrent.RecordBuffer.DID_NOT_CLAIM_RECORD;

/**
 * Stores a cache of the last sent sequence number.
 * <p>
 * Each instance is not thread-safe, however, they can share a common
 * off-heap in a threadsafe manner.
 */
// TODO: add a position and scan until you're at the latest position when reading from the table
public class SequenceNumbers implements Index
{
    public static final int NONE = -1;

    private static final int MASK = 0xFFFF;

    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final AsciiFlyweight asciiFlyweight = new AsciiFlyweight();
    private final SequenceNumberHeaderEncoder headerEncoder = new SequenceNumberHeaderEncoder();
    private final SequenceNumberHeaderDecoder headerDecoder = new SequenceNumberHeaderDecoder();
    private final LastKnownSequenceNumberEncoder lastKnownEncoder = new LastKnownSequenceNumberEncoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final int lastKnownBlockLength = lastKnownEncoder.sbeBlockLength();
    private final int lastKnownSchemaVersion = lastKnownEncoder.sbeSchemaVersion();

    private final RecordBuffer recordBuffer;
    private final AtomicBuffer outputBuffer;
    private final ErrorHandler errorHandler;
    private final boolean isWriter;

    public static SequenceNumbers forWriting(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        return new SequenceNumbers(outputBuffer, errorHandler, true).initialise();
    }

    public static SequenceNumbers forReading(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        return new SequenceNumbers(outputBuffer, errorHandler, false);
    }

    SequenceNumbers(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler, final boolean isWriter)
    {
        this.outputBuffer = outputBuffer;
        this.errorHandler = errorHandler;
        this.isWriter = isWriter;
        recordBuffer = new RecordBuffer(outputBuffer, 4, LastKnownSequenceNumberEncoder.BLOCK_LENGTH);
    }

    public SequenceNumbers initialise()
    {
        // TODO: only init if new buffer
        recordBuffer.initialise();
        // TODO
        return this;
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

            final int msgSeqNum = fixHeader.msgSeqNum();
            final long sessionId = messageFrame.session();
            final int key = hash(sessionId);
            final int claimedOffset = recordBuffer.claimRecord(key);

            if (claimedOffset == DID_NOT_CLAIM_RECORD)
            {
                errorHandler.onError(new IllegalStateException("Unable to claim an offset"));
                return;
            }

            lastKnownEncoder
                .wrap(outputBuffer, claimedOffset)
                .sessionId(sessionId)
                .sequenceNumber(msgSeqNum);

            recordBuffer.commit(claimedOffset);
        }
    }

    public int lastKnownSequenceNumber(final long sessionId)
    {
        stashedSequenceNumber = NONE;

        final int key = hash(sessionId);
        recordBuffer.forEach((recordKey, offset) ->
        {
            if (recordKey == key)
            {
                lastKnownDecoder.wrap(outputBuffer, offset, lastKnownBlockLength, lastKnownSchemaVersion);
                if (lastKnownDecoder.sessionId() == sessionId)
                {
                    stashedSequenceNumber = lastKnownDecoder.sequenceNumber();
                }
            }
        });

        return stashedSequenceNumber;
    }

    private int hash(long sessionId)
    {
        return Hashing.hash(sessionId, MASK);
    }

    private int stashedSequenceNumber;

    @Override
    public void close()
    {
        if (isWriter)
        {
            IoUtil.unmap(outputBuffer.byteBuffer());
        }
    }

}
