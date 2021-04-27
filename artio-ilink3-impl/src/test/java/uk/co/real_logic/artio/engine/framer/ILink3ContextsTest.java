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
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.MappedFile;

import java.io.File;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_FIXP_ID_FILE;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_SESSION_ID_BUFFER_SIZE;

public class ILink3ContextsTest
{
    public static final int PORT = 1;
    public static final String HOST = "host";
    public static final String ACCESS_KEY_ID = "key";

    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private File file;
    private FixPContexts contexts;

    @Before
    public void setup()
    {
        file = new File(DEFAULT_FIXP_ID_FILE).getAbsoluteFile();
        if (file.exists())
        {
            assertTrue(file.delete());
        }

        newContexts();
    }

    private void newContexts()
    {
        final MappedFile mappedFile = MappedFile.map(file.getPath(), DEFAULT_SESSION_ID_BUFFER_SIZE);
        contexts = new FixPContexts(mappedFile, errorHandler, new OffsetEpochNanoClock());
    }

    @Test
    public void shouldLoadSavedUuid()
    {
        final ILink3Context oldUuid = calculateUuid(true);
        final long firstUuid = oldUuid.connectUuid();
        assertEquals(0, oldUuid.connectLastUuid());
        assertTrue(oldUuid.newlyAllocated());
        onInitiatorNegotiateResponse(oldUuid, contexts);

        final int offset = contexts.offset();

        final ILink3Context secondUuid = calculateUuid(true);
        assertFalse(secondUuid.newlyAllocated());
        assertEquals(firstUuid, secondUuid.connectLastUuid());
        assertEquals(firstUuid, secondUuid.connectUuid());
        onInitiatorNegotiateResponse(secondUuid, contexts);

        assertOffset(offset);

        contexts.close();
        newContexts();

        final ILink3Context reloadedUuid = calculateUuid(true);
        assertEquals(firstUuid, reloadedUuid.connectLastUuid());
        assertEquals(firstUuid, reloadedUuid.connectUuid());
        assertFalse(reloadedUuid.newlyAllocated());

        assertOffset(offset);
    }

    @Test
    public void shouldGenerateNewUuidForReinitializationCase()
    {
        final ILink3Context oldUuid = calculateUuid(false);
        final long firstUuid = oldUuid.connectUuid();
        assertEquals(0, oldUuid.connectLastUuid());
        assertTrue(oldUuid.newlyAllocated());
        onInitiatorNegotiateResponse(oldUuid, contexts);

        final int offset = contexts.offset();

        final ILink3Context secondUuid = calculateUuid(false);
        assertTrue(secondUuid.newlyAllocated());
        assertEquals(firstUuid, secondUuid.connectLastUuid());
        assertNotEquals(firstUuid, secondUuid.connectUuid());
        onInitiatorNegotiateResponse(secondUuid, contexts);

        assertOffset(offset);
    }

    @Test
    public void shouldRegenerateUuid()
    {
        final ILink3Context oldUuid = calculateUuid(false);
        final long firstUuid = oldUuid.connectUuid();
        assertEquals(0, oldUuid.connectLastUuid());
        assertTrue(oldUuid.newlyAllocated());
        onInitiatorNegotiateResponse(oldUuid, contexts);

        final int offset = contexts.offset();

        final ILink3Context secondUuid = calculateUuid(true);
        assertFalse(secondUuid.newlyAllocated());
        assertEquals(firstUuid, secondUuid.connectLastUuid());
        assertEquals(firstUuid, secondUuid.connectUuid());
        onInitiatorNegotiateResponse(secondUuid, contexts);

        assertOffset(offset);

        contexts.close();
        newContexts();

        final ILink3Context reloadedUuid = calculateUuid(false);
        final long secondUuidValue = reloadedUuid.connectUuid();
        assertTrue(reloadedUuid.newlyAllocated());
        assertEquals(firstUuid, reloadedUuid.connectLastUuid());
        assertNotEquals(firstUuid, secondUuidValue);
        onInitiatorNegotiateResponse(reloadedUuid, contexts);

        assertOffset(offset);

        final ILink3Context reloadedUuid2 = calculateUuid(true);
        assertFalse(reloadedUuid2.newlyAllocated());
        assertEquals(secondUuidValue, reloadedUuid2.connectLastUuid());
        assertEquals(secondUuidValue, reloadedUuid2.connectUuid());
        onInitiatorNegotiateResponse(reloadedUuid2, contexts);

        assertOffset(offset);
    }

    @Test
    public void shouldReloadNotReestablishedUuidAfterRestart()
    {
        final ILink3Context firstContext = calculateUuid(false);
        final long firstUuid = firstContext.connectUuid();
        onInitiatorNegotiateResponse(firstContext, contexts);

        final ILink3Context secondContext = calculateUuid(false);
        final long secondUuid = secondContext.connectUuid();
        assertEquals(firstUuid, secondContext.connectLastUuid());
        assertTrue(secondContext.newlyAllocated());
        assertNotEquals(firstUuid, secondUuid);
        onInitiatorNegotiateResponse(secondContext, contexts);

        final int offset = contexts.offset();

        contexts.close();
        newContexts();

        final ILink3Context thirdContext = calculateUuid(true);
        assertFalse(thirdContext.newlyAllocated());
        assertEquals(secondUuid, thirdContext.connectLastUuid());
        assertEquals(secondUuid, thirdContext.connectUuid());
        onInitiatorNegotiateResponse(thirdContext, contexts);

        assertOffset(offset);
    }

    private void onInitiatorNegotiateResponse(final ILink3Context context, final FixPContexts contexts)
    {
        if (context.onInitiatorNegotiateResponse())
        {
            contexts.saveNewContext(context);
        }
        else
        {
            contexts.updateContext(context);
        }
    }

    // Assert that we're not repeatedly growing the file with many reconnects with new offsets.
    private void assertOffset(final int offset)
    {
        assertEquals(offset, contexts.offset());
    }

    private ILink3Context calculateUuid(final boolean reestablishConnection)
    {
        final ILink3Key key = new ILink3Key(PORT, HOST, ACCESS_KEY_ID);
        return (ILink3Context)contexts.calculateInitiatorContext(key, reestablishConnection);
    }

    @After
    public void close()
    {
        contexts.close();
        assertTrue(file.delete());
    }
}
