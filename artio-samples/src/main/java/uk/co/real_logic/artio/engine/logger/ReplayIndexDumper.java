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

import org.agrona.LangUtil;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Prints out the state of a replay index file.
 */
public final class ReplayIndexDumper
{
    public static void main(final String[] args) throws IOException
    {
        final File file = new File(args[0]);
        final String output = "replay-index-dump.csv";
        try (BufferedWriter out = new BufferedWriter(new FileWriter(output)))
        {
            out.write("beginPosition,sequenceIndex,sequenceNumber,recordingId,readLength\n");

            ReplayIndexExtractor.extract(file, new ReplayIndexExtractor.ReplayIndexHandler()
            {
                public void onEntry(final ReplayIndexRecordDecoder indexRecord)
                {
                    final long beginPosition = indexRecord.position();
                    final int sequenceIndex = indexRecord.sequenceIndex();
                    final int sequenceNumber = indexRecord.sequenceNumber();
                    final long recordingId = indexRecord.recordingId();
                    final int readLength = indexRecord.length();

                    try
                    {
                        out.write(
                            beginPosition + "," +
                            sequenceIndex + "," +
                            sequenceNumber + "," +
                            recordingId + "," +
                            readLength + "\n");
                    }
                    catch (final IOException e)
                    {
                        LangUtil.rethrowUnchecked(e);
                    }
                }

                public void onLapped()
                {
                    System.err.println("Error: lapped by writer currently updating the file");
                }
            });
        }

        final ReplayIndexExtractor.ReplayIndexValidator validator = new ReplayIndexExtractor.ReplayIndexValidator();
        ReplayIndexExtractor.extract(file, validator);

        final List<ReplayIndexExtractor.ValidationError> errors = validator.errors();
        errors.forEach(System.err::println);
    }
}
