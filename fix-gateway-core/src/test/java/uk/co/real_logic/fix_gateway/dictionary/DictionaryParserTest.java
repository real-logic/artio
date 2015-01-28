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
package uk.co.real_logic.fix_gateway.dictionary;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Value;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DictionaryParserTest
{
    private static final String EXAMPLE_FILE = "example_dictionary.xml";

    private DataDictionary dictionary;

    @Before
    public void setUp() throws Exception
    {
        dictionary = parseExample();
    }

    @Test
    public void parsesExampleDictionary()
    {
        assertNotNull("Missing dictionary", dictionary);
        assertNotNull("Missing messages", dictionary.messages());
        assertNotNull("Missing fields", dictionary.fields());
    }

    @Test
    public void parsesSimpleField()
    {
        final Field bodyLength = field("BodyLength");

        assertNotNull("Hasn't found BodyLength", bodyLength);
        assertEquals("BodyLength", bodyLength.name());
        assertEquals(9, bodyLength.number());
        assertEquals(Type.INT, bodyLength.type());
    }

    @Test
    public void parsesAllFields()
    {
        assertEquals(32, dictionary.fields().size());
    }

    @Test
    public void parsesEnumField()
    {
        final Field msgType = field("MsgType");

        assertNotNull("Hasn't found MsgType", msgType);
        assertEquals("MsgType", msgType.name());
        assertEquals(35, msgType.number());
        assertEquals(Type.STRING, msgType.type());

        final List<Value> values = msgType.values();

        assertEquals(new Value('0', "HEARTBEAT"), values.get(0));
        assertEquals(new Value('8', "EXECUTION_REPORT"), values.get(1));
        assertEquals(new Value('D', "ORDER_SINGLE"), values.get(2));
    }

    @Test
    public void parsesAllEnum()
    {
        assertEquals(9, countEnumFields());
    }

    @Test
    public void parseSimpleMessage()
    {

    }

    private long countEnumFields()
    {
        return dictionary
                .fields()
                .values()
                .stream()
                .filter(Field::isEnum)
                .count();
    }

    private Field field(final String name)
    {
        return dictionary.fields().get(name);
    }

    private DataDictionary parseExample() throws Exception
    {
        return new DictionaryParser().parse(DictionaryParserTest.class.getResourceAsStream(EXAMPLE_FILE));
    }

}
