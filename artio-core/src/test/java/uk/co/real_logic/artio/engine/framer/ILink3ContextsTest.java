/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.MappedFile;

import java.io.File;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_ILINK3_ID_FILE;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_SESSION_ID_BUFFER_SIZE;

public class ILink3ContextsTest
{
    public static final int PORT = 1;
    public static final String HOST = "host";
    public static final String ACCESS_KEY_ID = "key";

    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private File file;
    private MappedFile mappedFile;
    private ILink3Contexts contexts;

    @Before
    public void setup()
    {
        file = new File(DEFAULT_ILINK3_ID_FILE).getAbsoluteFile();
        if (file.exists())
        {
            assertTrue(file.delete());
        }

        newContexts();
    }

    private void newContexts()
    {
        mappedFile = MappedFile.map(file.getPath(), DEFAULT_SESSION_ID_BUFFER_SIZE);
        contexts = new ILink3Contexts(mappedFile, errorHandler);
    }

    @Test
    public void shouldLoadSavedUuid()
    {
        final long oldUuid = calculateUuid();
        final long secondUuid = calculateUuid();
        assertEquals(oldUuid, secondUuid);

        contexts.close();
        newContexts();

        final long reloadedUuid = calculateUuid();
        assertEquals(oldUuid, reloadedUuid);
    }

    @Test
    public void shouldRegenerateUuid()
    {
        final long oldUuid = calculateUuid(false);
        final long secondUuid = calculateUuid(true);
        assertEquals(oldUuid, secondUuid);

        contexts.close();
        newContexts();

        final long reloadedUuid = calculateUuid(false);
        assertNotEquals(oldUuid, reloadedUuid);

        final long reloadedUuid2 = calculateUuid(true);
        assertEquals(reloadedUuid, reloadedUuid2);
    }

    private long calculateUuid()
    {
        return calculateUuid(true);
    }

    private long calculateUuid(final boolean reestablishConnection)
    {
        return contexts.calculateUuid(PORT, HOST, ACCESS_KEY_ID, reestablishConnection);
    }

    @After
    public void close()
    {
        contexts.close();
        assertTrue(file.delete());
    }
}
