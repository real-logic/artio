/*
 * Copyright 2015=2016 Real Logic Ltd.
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
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.engine.MappedFile;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.File;

import static uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexDescriptor.RECORD_SIZE;
import static uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberEncoder.SCHEMA_VERSION;

// TODO: 3. Test & Update Configuration: only update the file buffer when you rotate the term buffer
// TODO: 4. apply the alignment and checksumming rules
// TODO: 5. rescan the last term buffer upon restart, account for other file location
public class SequenceNumberIndexWriter implements Index
{
    private static final long MISSING_RECORD = -1L;
    private static final long UNINITIALISED = -1;
    private static final int SEQUENCE_NUMBER_OFFSET = 8;

    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final MessageHeaderDecoder fileHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder fileHeaderEncoder = new MessageHeaderEncoder();
    private final LastKnownSequenceNumberEncoder lastKnownEncoder = new LastKnownSequenceNumberEncoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final Long2LongHashMap recordOffsets = new Long2LongHashMap(MISSING_RECORD);

    private final AtomicBuffer inMemoryBuffer;
    private final ErrorHandler errorHandler;
    private final File indexPath;
    private final File writablePath;
    private final File passingPlacePath;

    private MappedFile writableFile;
    private MappedFile indexFile;
    private long nextRollPosition = UNINITIALISED;

    public SequenceNumberIndexWriter(
        final AtomicBuffer inMemoryBuffer,
        final MappedFile indexFile,
        final ErrorHandler errorHandler)
    {
        this.inMemoryBuffer = inMemoryBuffer;
        this.indexFile = indexFile;

        final String indexFilePath = indexFile.file().getAbsolutePath();
        indexPath = indexFile.file();
        writablePath = new File(indexFilePath + "-writable");
        passingPlacePath = new File(indexFilePath + "-passing");
        writableFile = MappedFile.map(writablePath, indexFile.buffer().capacity());

        // TODO: Fsync parent directory
        this.errorHandler = errorHandler;
        initialiseBuffer();
    }

    public void indexRecord(
        final DirectBuffer buffer,
        final int srcOffset,
        final int length,
        final int streamId,
        final int aeronSessionId,
        final long position)
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

            saveRecord(msgSeqNum, sessionId);
        }

        checkTermRoll(buffer, srcOffset, position, length);
    }

    private void checkTermRoll(final DirectBuffer buffer, final int offset, final long position, final int length)
    {
        final long termBufferLength = buffer.capacity();
        if (nextRollPosition == UNINITIALISED)
        {
            nextRollPosition = position + termBufferLength - offset;
        }
        else if ((position + length) > nextRollPosition)
        {
            nextRollPosition += termBufferLength;
            updateFile();
        }
    }

    private void updateFile()
    {
        // updateChecksum();
        saveFile();
        flipFiles();
    }

    private void saveFile()
    {
        // TODO: optimise to overwrite only needed bytes
        writableFile.buffer().putBytes(0, inMemoryBuffer, 0, inMemoryBuffer.capacity());
        writableFile.force();
    }

    private void flipFiles()
    {
        final boolean flipsFiles = rename(indexPath, passingPlacePath)
                                && rename(writablePath, indexPath)
                                && rename(passingPlacePath, writablePath);

        if (flipsFiles)
        {
            final MappedFile file = this.writableFile;
            writableFile = indexFile;
            indexFile = file;
        }
    }

    private boolean rename(final File src, final File dest)
    {
        if (src.renameTo(dest))
        {
            return true;
        }

        errorHandler.onError(new IllegalStateException("unable to rename " + src + " to " + dest));
        return false;
    }

    public boolean isOpen()
    {
        return writableFile.isOpen();
    }

    public void close()
    {
        if (isOpen())
        {
            try
            {
                updateFile();
            }
            finally
            {
                indexFile.close();
                writableFile.close();
            }
        }
    }

    public boolean clear()
    {
        return indexPath.delete()
            && writablePath.delete()
            && (!passingPlacePath.exists() || passingPlacePath.delete());
    }

    private void saveRecord(final int newSequenceNumber, final long sessionId)
    {
        int position = (int) recordOffsets.get(sessionId);
        if (position == MISSING_RECORD)
        {
            final int lastRecordOffset = inMemoryBuffer.capacity() - RECORD_SIZE;
            position = SequenceNumberIndexDescriptor.HEADER_SIZE;
            while (position <= lastRecordOffset)
            {
                lastKnownDecoder.wrap(inMemoryBuffer, position, RECORD_SIZE, SCHEMA_VERSION);
                if (lastKnownDecoder.sequenceNumber() == 0)
                {
                    createNewRecord(newSequenceNumber, sessionId, position);
                    return;
                }
                else if (lastKnownDecoder.sessionId() == sessionId)
                {
                    sequenceNumberOrdered(position, newSequenceNumber);
                    return;
                }

                position += RECORD_SIZE;
            }
        }
        else
        {
            sequenceNumberOrdered(position, newSequenceNumber);
            return;
        }

        errorHandler.onError(new IllegalStateException("Unable to claim an position"));
    }

    private void createNewRecord(final int sequenceNumber, final long sessionId, final int position)
    {
        recordOffsets.put(sessionId, position);
        lastKnownEncoder
            .wrap(inMemoryBuffer, position)
            .sessionId(sessionId);
        sequenceNumberOrdered(position, sequenceNumber);
    }

    private void initialiseBuffer()
    {
        final AtomicBuffer filebuffer = indexFile.buffer();
        final int fileCapacity = filebuffer.capacity();
        final int inMemoryCapacity = inMemoryBuffer.capacity();
        if (fileCapacity != inMemoryCapacity)
        {
            throw new IllegalStateException(String.format(
                "In memory buffer and disk file don't have the same size, disk: %d, memory: %d",
                fileCapacity,
                inMemoryCapacity
            ));
        }

        inMemoryBuffer.putBytes(0, filebuffer, 0, fileCapacity);

        LoggerUtil.initialiseBuffer(
            inMemoryBuffer,
            fileHeaderEncoder,
            fileHeaderDecoder,
            lastKnownEncoder.sbeSchemaId(),
            lastKnownEncoder.sbeTemplateId(),
            lastKnownEncoder.sbeSchemaVersion(),
            lastKnownEncoder.sbeBlockLength());
    }

    public void sequenceNumberOrdered(final int recordOffset, final int value)
    {
        inMemoryBuffer.putIntOrdered(recordOffset + SEQUENCE_NUMBER_OFFSET, value);
    }
}
