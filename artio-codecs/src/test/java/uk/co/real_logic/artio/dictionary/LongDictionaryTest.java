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
package uk.co.real_logic.artio.dictionary;

import org.agrona.collections.IntHashSet;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Field;
import uk.co.real_logic.artio.dictionary.ir.Field.Type;
import uk.co.real_logic.artio.dictionary.ir.Message;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class LongDictionaryTest
{
    private Dictionary data;

    @Before
    public void createDataDictionary()
    {
        final Message heartbeat = new Message("Hearbeat", "0", "admin");
        heartbeat.requiredEntry(new Field(115, "OnBehalfOfCompID", Type.STRING));
        heartbeat.optionalEntry(new Field(112, "TestReqID", Type.STRING));

        final List<Message> messages = Arrays.asList(heartbeat);
        data = new Dictionary(messages, null, null, null, null, "FIX", 4, 4);
    }

    @Test
    public void buildsValidationDictionaryForRequiredFields()
    {
        final LongDictionary longDictionary = LongDictionary.requiredFields(data);
        final IntHashSet heartbeat = longDictionary.values('0');

        assertThat(heartbeat, hasItem(115));
        assertThat(heartbeat, hasSize(1));
        assertTrue(longDictionary.contains('0', 115));
    }

    @Test
    public void buildsValidationDictionaryForAllFields()
    {
        final LongDictionary longDictionary = LongDictionary.allFields(data);
        final IntHashSet heartbeat = longDictionary.values('0');

        assertThat(heartbeat, hasItem(115));
        assertThat(heartbeat, hasItem(112));
        assertThat(heartbeat, hasSize(2));

        assertTrue(longDictionary.contains('0', 115));
        assertTrue(longDictionary.contains('0', 112));
    }
}
