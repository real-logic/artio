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
package uk.co.real_logic.fix_gateway.dictionary.generation;

import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.getMessageType;

public class GenerationUtilTest
{

    @Test
    public void shouldGenerateDifferentMessageTypeIdentifiers()
    {
        assertNotEquals(getMessageType("AL"), getMessageType("AM"));
        assertNotEquals(getMessageType("AL"), getMessageType("AN"));
        assertNotEquals(getMessageType("AR"), getMessageType("AQ"));
        assertNotEquals(getMessageType("BC"), getMessageType("BB"));
        assertNotEquals(getMessageType("BD"), getMessageType("BE"));
        assertNotEquals(getMessageType("BG"), getMessageType("BF"));
    }

}
