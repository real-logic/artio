/*
 * Copyright 2013 Real Logic Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.constantName;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.packMessageType;

public class GenerationUtilTest
{

    @Test
    public void shouldGenerateDifferentMessageTypeIdentifiers()
    {
        assertNotEquals(packMessageType("AL"), packMessageType("AM"));
        assertNotEquals(packMessageType("AL"), packMessageType("AN"));
        assertNotEquals(packMessageType("AR"), packMessageType("AQ"));
        assertNotEquals(packMessageType("BC"), packMessageType("BB"));
        assertNotEquals(packMessageType("BD"), packMessageType("BE"));
        assertNotEquals(packMessageType("BG"), packMessageType("BF"));
    }

    @Test
    public void shouldReplaceIDWithIdInConstantName()
    {
        assertEquals("DEFAULT_APPL_VER_ID", constantName("DefaultApplVerID"));
    }

    @Test
    public void shouldDropGroupCounterForNumberOfElementsInReaptingGroupConstant()
    {
        assertEquals("NO_MSG_TYPES", constantName("NoMsgTypesGroupCounter"));
    }
}
