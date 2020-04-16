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
import uk.co.real_logic.artio.util.OffsetEpochNanoClock;

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
        final MappedFile mappedFile = MappedFile.map(file.getPath(), DEFAULT_SESSION_ID_BUFFER_SIZE);
        contexts = new ILink3Contexts(mappedFile, errorHandler, new OffsetEpochNanoClock());
    }

    @Test
    public void shouldLoadSavedUuid()
    {
        final ILink3Context oldUuid = calculateUuid();
        assertTrue(oldUuid.newlyAllocated());

        final ILink3Context secondUuid = calculateUuid();
        assertFalse(secondUuid.newlyAllocated());
        assertEquals(oldUuid.uuid(), secondUuid.uuid());

        contexts.close();
        newContexts();

        final ILink3Context reloadedUuid = calculateUuid();
        assertEquals(oldUuid.uuid(), reloadedUuid.uuid());
        assertFalse(reloadedUuid.newlyAllocated());
    }

    @Test
    public void shouldRegenerateUuid()
    {
        final ILink3Context oldUuid = calculateUuid(false);
        assertTrue(oldUuid.newlyAllocated());

        final ILink3Context secondUuid = calculateUuid(true);
        assertFalse(secondUuid.newlyAllocated());
        assertEquals(oldUuid.uuid(), secondUuid.uuid());

        contexts.close();
        newContexts();

        final ILink3Context reloadedUuid = calculateUuid(false);
        assertTrue(reloadedUuid.newlyAllocated());
        assertNotEquals(oldUuid.uuid(), reloadedUuid.uuid());

        final ILink3Context reloadedUuid2 = calculateUuid(true);
        assertFalse(reloadedUuid2.newlyAllocated());
        assertEquals(reloadedUuid.uuid(), reloadedUuid2.uuid());
    }

    private ILink3Context calculateUuid()
    {
        return calculateUuid(true);
    }

    private ILink3Context calculateUuid(final boolean reestablishConnection)
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
