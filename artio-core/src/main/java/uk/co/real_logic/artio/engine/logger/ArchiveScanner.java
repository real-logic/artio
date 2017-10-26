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

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermReader;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.replication.StreamIdentifier;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Support for enumerating/filtering/compressing archives.
 */
public class ArchiveScanner
{
    private final LogDirectoryDescriptor directoryDescriptor;

    public ArchiveScanner(
        final String logFileDir)
    {
        this.directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
    }

    public void forEachFragment(
        final StreamIdentifier streamId,
        final FragmentHandler handler,
        final ErrorHandler errorHandler)
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(0, 0);
        for (final File logFile : directoryDescriptor.listLogFiles(streamId))
        {
            final ByteBuffer byteBuffer = LoggerUtil.mapExistingFile(logFile);
            if (byteBuffer.capacity() > 0)
            {
                termBuffer.wrap(byteBuffer);
                final int initialTermId = LogBufferDescriptor.initialTermId(termBuffer);
                final Header header = new Header(initialTermId, termBuffer.capacity());
                TermReader.read(
                    termBuffer,
                    0,
                    handler,
                    Integer.MAX_VALUE,
                    header,
                    errorHandler);
            }
        }
    }

}
