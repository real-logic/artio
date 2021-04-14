/*
 * Copyright 2020 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine;

import org.agrona.IoUtil;
import org.junit.*;

import java.io.File;

import static org.junit.Assert.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS;

public class DuplicateEngineCheckerTest
{
    private static final File FILE = new File(DuplicateEngineChecker.FILE_NAME);
    private static final String THIS_DIR = new File(".").getAbsolutePath();

    private DuplicateEngineChecker oldEngine;
    private DuplicateEngineChecker newEngine;

    @Before
    public void startup()
    {
        IoUtil.deleteIfExists(FILE);
    }

    @After
    public void teardown()
    {
        if (oldEngine != null)
        {
            oldEngine.finalClose();
        }
        if (newEngine != null)
        {
            newEngine.finalClose();
        }
    }

    @Test
    public void shouldDetectDuplicate()
    {
        oldEngine = new DuplicateEngineChecker(
            DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS, THIS_DIR, true);
        oldEngine.check();

        newEngine = new DuplicateEngineChecker(
            DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS, THIS_DIR, true);

        assertCheckThrows(newEngine);
    }

    @Test
    public void shouldNotDetectDuplicateAfterTimeout() throws InterruptedException
    {
        final long timeoutInMs = 10;
        oldEngine = new DuplicateEngineChecker(
            timeoutInMs, THIS_DIR, true);
        oldEngine.check();
        // Simulate unclean shutdown - where the old process ends without removing the file :. no finalClose().
        // Need to unmap to avoid windows file locking errors in tests.
        oldEngine.unmap();

        Thread.sleep(timeoutInMs);

        newEngine = new DuplicateEngineChecker(
            timeoutInMs, THIS_DIR, true);
        newEngine.check();
    }

    @Test
    public void shouldUpdateTimeoutOnDutyCycle() throws InterruptedException
    {
        final long timeoutInMs = 100;
        oldEngine = new DuplicateEngineChecker(
            timeoutInMs, THIS_DIR, true);
        oldEngine.check();

        Thread.sleep(timeoutInMs);
        int work = 0;
        while (work < 1)
        {
            work += oldEngine.doWork();
            Thread.sleep(1);
        }

        newEngine = new DuplicateEngineChecker(
            timeoutInMs, THIS_DIR, true);
        assertCheckThrows(newEngine);
    }

    @Test
    public void shouldRemoveFileOnShutdown()
    {
        final DuplicateEngineChecker engine = new DuplicateEngineChecker(
            DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS, THIS_DIR, true);
        engine.check();

        assertTrue(FILE.exists());

        engine.finalClose();

        assertFalse(FILE.exists());
    }

    private void assertCheckThrows(final DuplicateEngineChecker newEngine)
    {
        Assert.assertThrows(
            "Error starting Engine a duplicate Artio Engine instance might be running",
            IllegalStateException.class,
            newEngine::check);
    }
}
