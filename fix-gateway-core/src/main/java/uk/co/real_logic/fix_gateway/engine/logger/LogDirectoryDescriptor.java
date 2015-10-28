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

import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class LogDirectoryDescriptor
{
    private static final int EXTENSION_LENGTH = ".log".length();

    private final String logFileDir;
    private final String logFileFormat;
    private final String metaDataLogFileFormat;

    public LogDirectoryDescriptor(final String logFileDir)
    {
        this.logFileDir = logFileDir;
        logFileFormat = logFileDir + File.separator + "archive_%s_%d_%d_%d.log";
        metaDataLogFileFormat = logFileDir + File.separator + "meta-data_%s_%d_%d.log";
    }

    public File logFile(final StreamIdentifier stream, final int sessionId, final int termId)
    {
        return new File(String.format(logFileFormat, stream.channel(), stream.streamId(), sessionId, termId));
    }

    public File metaDataLogFile(final StreamIdentifier stream, final int sessionId)
    {
        return new File(String.format(metaDataLogFileFormat, stream.channel(), stream.streamId(), sessionId));
    }

    public List<File> listLogFiles(final int streamId)
    {
        final String prefix = String.format("archive_%d", streamId);
        final File logFileDir = new File(this.logFileDir);
        return Arrays.asList(logFileDir.listFiles(file ->
        {
            return file.getName().startsWith(prefix);
        }));
    }

    public int computeTermId(final File logFile)
    {
        final String logFileName = logFile.getName();
        final int startOfTermId = logFileName.lastIndexOf('-');
        final int endOfTermId = logFileName.length() - EXTENSION_LENGTH;
        return Integer.parseInt(logFileName.substring(startOfTermId, endOfTermId));
    }
}
