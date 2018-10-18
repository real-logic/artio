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

import uk.co.real_logic.artio.protocol.StreamIdentifier;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class LogDirectoryDescriptor
{

    private final String logFileDir;

    public LogDirectoryDescriptor(final String logFileDir)
    {
        this.logFileDir = logFileDir;
    }

    public List<File> listLogFiles(final StreamIdentifier stream)
    {
        final String prefix = String.format("archive_%s_%d", stream.canonicalForm(), stream.streamId());
        final File logFileDir = new File(this.logFileDir);
        return Arrays.asList(logFileDir.listFiles(file -> file.getName().startsWith(prefix)));
    }

}
