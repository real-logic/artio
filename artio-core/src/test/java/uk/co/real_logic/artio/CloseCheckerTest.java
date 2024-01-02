/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio;

import org.junit.After;
import org.junit.Test;

public class CloseCheckerTest
{
    private static final String RESOURCE_ID = "resource";
    private static final String OWNER_ID = "owner";

    static
    {
        System.setProperty("fix.core.close_checker", "true");
    }

    @After
    public void tearDown()
    {
        CloseChecker.onClose(RESOURCE_ID, OWNER_ID);
    }

    @Test(expected = Error.class)
    public void shouldNotifyWhenALibraryIsOpenedWhenOpen()
    {
        CloseChecker.onOpen(RESOURCE_ID, OWNER_ID);

        CloseChecker.validate(RESOURCE_ID);
    }

    @Test
    public void shouldNotInterfereWithoutValidation()
    {
        CloseChecker.onOpen(RESOURCE_ID, OWNER_ID);
    }

    @Test(expected = Error.class)
    public void shouldRecogniseDoubleOpens()
    {
        CloseChecker.onOpen(RESOURCE_ID, OWNER_ID);

        CloseChecker.onOpen(RESOURCE_ID, OWNER_ID);

        CloseChecker.onClose(RESOURCE_ID, OWNER_ID);

        CloseChecker.validate(RESOURCE_ID);
    }
}
