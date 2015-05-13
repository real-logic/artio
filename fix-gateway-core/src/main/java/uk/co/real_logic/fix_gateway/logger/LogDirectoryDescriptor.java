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
package uk.co.real_logic.fix_gateway.logger;

import java.io.File;

public final class LogDirectoryDescriptor
{
    public static final String LOG_FILE_DIR_PROP = "logging.dir";

    public static final String LOG_FILE_DIR_DEFAULT = "logs";

    public static final String LOG_FILE_DIR = System.getProperty(LOG_FILE_DIR_PROP, LOG_FILE_DIR_DEFAULT);

    public static String logFile(final int streamId, final int termId)
    {
        return String.format(LOG_FILE_DIR + File.separator + "archive-%d-%d.log", streamId, termId);
    }

    public static String metaDatalogFile(final int streamId)
    {
        return String.format(LOG_FILE_DIR + File.separator + "meta-data-%d.log", streamId);
    }

    public static final String INDEX_FILE_SIZE_PROP = "logging.index.size";

    public static final int INDEX_FILE_SIZE_DEFAULT = 2 * 1024 * 1024;

    public static final int INDEX_FILE_SIZE = Integer.getInteger(INDEX_FILE_SIZE_PROP, INDEX_FILE_SIZE_DEFAULT);

}
