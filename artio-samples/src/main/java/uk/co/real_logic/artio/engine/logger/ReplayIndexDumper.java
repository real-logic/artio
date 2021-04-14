/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;

/**
 * Prints out the state of a replay index file.
 */
public final class ReplayIndexDumper
{
    public static void main(final String[] args) throws IOException
    {
        final String fileName = args[0];
        final String output = "replay-index-dump.csv";
        final MappedByteBuffer mappedByteBuffer = LoggerUtil.mapExistingFile(new File(fileName));
        final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

        final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
        final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

        messageFrameHeader.wrap(buffer, 0);
        final int actingBlockLength = messageFrameHeader.blockLength();
        final int actingVersion = messageFrameHeader.version();

        final int capacity = recordCapacity(buffer.capacity());

        long iteratorPosition = beginChangeVolatile(buffer);
        long stopIteratingPosition = iteratorPosition + capacity;

        try (BufferedWriter out = new BufferedWriter(new FileWriter(output)))
        {
            out.write("beginPosition,sequenceIndex,sequenceNumber,recordingId,readLength\n");

            while (iteratorPosition < stopIteratingPosition)
            {
                final long changePosition = endChangeVolatile(buffer);

                if (changePosition > iteratorPosition && (iteratorPosition + capacity) <= beginChangeVolatile(buffer))
                {
                    System.err.println("Internal state error in file: lapped by writer condition met");
                    iteratorPosition = changePosition;
                    stopIteratingPosition = iteratorPosition + capacity;
                }

                final int offset = offset(iteratorPosition, capacity);
                indexRecord.wrap(buffer, offset, actingBlockLength, actingVersion);
                final long beginPosition = indexRecord.position();
                final int sequenceIndex = indexRecord.sequenceIndex();
                final int sequenceNumber = indexRecord.sequenceNumber();
                final long recordingId = indexRecord.recordingId();
                final int readLength = indexRecord.length();

                if (beginPosition == 0)
                {
                    break;
                }

                out.write(
                    beginPosition + "," +
                    sequenceIndex + "," +
                    sequenceNumber + "," +
                    recordingId + "," +
                    readLength + "\n");

                iteratorPosition += RECORD_LENGTH;
            }
        }
    }
}
