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
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.concurrent.RecordBuffer.DID_NOT_CLAIM_RECORD;

/**
 * Stores a cache of the last sent sequence number.
 * <p>
 * Each instance is not thread-safe, however, they can share a common
 * off-heap in a threadsafe manner.
 */
public class SequenceNumberIndex implements Index
{
    public static final int NONE = -1;

    private static final int MASK = 0xFFFF;
    private static final int HEADER_SIZE = SIZE_OF_INT;
    private static final int KNOWN_STREAM_POSITION_INDEX = 0;

    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final LastKnownSequenceNumberEncoder lastKnownEncoder = new LastKnownSequenceNumberEncoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final int lastKnownBlockLength = lastKnownEncoder.sbeBlockLength();
    private final int lastKnownSchemaVersion = lastKnownEncoder.sbeSchemaVersion();

    private final RecordBuffer recordBuffer;
    private final AtomicBuffer outputBuffer;
    private final ErrorHandler errorHandler;
    private final boolean isWriter;

    public static SequenceNumberIndex forWriting(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        return new SequenceNumberIndex(outputBuffer, errorHandler, true).initialise();
    }

    public static SequenceNumberIndex forReading(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        return new SequenceNumberIndex(outputBuffer, errorHandler, false);
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

            asciiBuffer.wrap(buffer);
            fixHeader.decode(asciiBuffer, offset, messageFrame.bodyLength());

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

            knownStreamPosition(srcOffset + length);
        }
    }

    public int lastKnownSequenceNumber(final long sessionId)
    {
        final int key = hash(sessionId);
        final int offset = recordBuffer.get(key);
        if (offset != RecordBuffer.DID_NOT_CLAIM_RECORD)
        {
            lastKnownDecoder.wrap(outputBuffer, offset, lastKnownBlockLength, lastKnownSchemaVersion);
            if (lastKnownDecoder.sessionId() == sessionId)
            {
                return lastKnownDecoder.sequenceNumber();
            }
        }

        return NONE;
    }

    @Override
    public void close()
    {
        if (isWriter)
        {
            IoUtil.unmap(outputBuffer.byteBuffer());
        }
    }

    SequenceNumberIndex(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler, final boolean isWriter)
    {
        this.outputBuffer = outputBuffer;
        this.errorHandler = errorHandler;
        this.isWriter = isWriter;
        recordBuffer = new RecordBuffer(outputBuffer, HEADER_SIZE, LastKnownSequenceNumberEncoder.BLOCK_LENGTH);
    }

    private SequenceNumberIndex initialise()
    {
        if (knownStreamPosition() == 0)
        {
            recordBuffer.initialise();
        }
        return this;
    }

    private void knownStreamPosition(final int position)
    {
        outputBuffer.putIntOrdered(KNOWN_STREAM_POSITION_INDEX, position);
    }

    private int knownStreamPosition()
    {
        return outputBuffer.getIntVolatile(KNOWN_STREAM_POSITION_INDEX);
    }

    private int hash(long sessionId)
    {
        return Hashing.hash(sessionId, MASK);
    }

}
