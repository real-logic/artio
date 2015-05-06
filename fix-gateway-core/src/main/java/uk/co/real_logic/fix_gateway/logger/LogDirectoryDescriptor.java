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

final class LogDirectoryDescriptor
{
    public static final String LOG_FILE_DIR_PROP = "logging.dir";

    public static final String LOG_FILE_DIR_DEFAULT = "logs";

    public static final String LOG_FILE_DIR = System.getProperty(LOG_FILE_DIR_PROP, LOG_FILE_DIR_DEFAULT);

    public static File logFile(final int id)
    {
        return new File(String.format(LOG_FILE_DIR + File.separator + "data-%d.log", id));
    }

    public static File indexFile(final String indexName, final int id)
    {
        return new File(String.format(LOG_FILE_DIR + File.separator + "index-%s-%d.log", indexName, id));
    }

    public static final String INDEX_FILE_SIZE_PROP = "logging.index.size";

    public static final long INDEX_FILE_SIZE_DEFAULT = 2 * 1024 * 1024;

    public static final long INDEX_FILE_SIZE = Long.getLong(INDEX_FILE_SIZE_PROP, INDEX_FILE_SIZE_DEFAULT);

    public static final String LOG_FILE_SIZE_PROP = "logging.file.size";

    public static final long LOG_FILE_SIZE_DEFAULT = 2 * 1024 * 1024;

    public static final long LOG_FILE_SIZE = Long.getLong(LOG_FILE_SIZE_PROP, LOG_FILE_SIZE_DEFAULT);
}
