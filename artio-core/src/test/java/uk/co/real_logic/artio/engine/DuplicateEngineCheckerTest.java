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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS;

public class DuplicateEngineCheckerTest
{
    private static final File FILE = new File(DuplicateEngineChecker.FILE_NAME);

    @Before
    public void startup()
    {
        IoUtil.deleteIfExists(FILE);
    }

    @Test
    public void shouldDetectDuplicate()
    {
        final String thisDir = new File(".").getAbsolutePath();
        final DuplicateEngineChecker oldEngine = new DuplicateEngineChecker(
            DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS, thisDir);
        oldEngine.check();

        final DuplicateEngineChecker newEngine = new DuplicateEngineChecker(
            DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS, thisDir);

        assertCheckThrows(newEngine);
    }

    @Test
    public void shouldNotDetectDuplicateAfterTimeout() throws InterruptedException
    {
        final long timeoutInMs = 10;
        final String thisDir = new File(".").getAbsolutePath();
        final DuplicateEngineChecker oldEngine = new DuplicateEngineChecker(
            timeoutInMs, thisDir);
        oldEngine.check();

        Thread.sleep(timeoutInMs);

        final DuplicateEngineChecker newEngine = new DuplicateEngineChecker(
            timeoutInMs, thisDir);
        newEngine.check();
    }

    @Test
    public void shouldUpdateTimeoutOnDutyCycle() throws InterruptedException
    {
        final long timeoutInMs = 100;
        final String thisDir = new File(".").getAbsolutePath();
        final DuplicateEngineChecker oldEngine = new DuplicateEngineChecker(
            timeoutInMs, thisDir);
        oldEngine.check();

        Thread.sleep(timeoutInMs);
        assertEquals(1, oldEngine.doWork());

        final DuplicateEngineChecker newEngine = new DuplicateEngineChecker(
            timeoutInMs, thisDir);
        assertCheckThrows(newEngine);
    }

    @Test
    public void shouldRemoveFileOnShutdown()
    {
        final String thisDir = new File(".").getAbsolutePath();
        final DuplicateEngineChecker engine = new DuplicateEngineChecker(
            DEFAULT_DUPLICATE_ENGINE_TIMEOUT_IN_MS, thisDir);
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
