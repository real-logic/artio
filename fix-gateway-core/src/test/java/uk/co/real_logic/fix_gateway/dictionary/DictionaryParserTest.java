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
import uk.co.real_logic.fix_gateway.dictionary.ir.Group;
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Category.ADMIN;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type.STRING;

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
    public void shouldParseExampleDictionary()
    {
        assertNotNull("Missing dictionary", dictionary);
        assertNotNull("Missing messages", dictionary.messages());
        assertNotNull("Missing fields", dictionary.fields());
    }

    @Test
    public void shouldParseSimpleField()
    {
        final Field bodyLength = field("BodyLength");

        assertNotNull("Hasn't found BodyLength", bodyLength);
        assertEquals("BodyLength", bodyLength.name());
        assertEquals(9, bodyLength.number());
        assertEquals(Type.INT, bodyLength.type());
    }

    @Test
    public void shouldParseAllFields()
    {
        assertEquals(36, dictionary.fields().size());
    }

    @Test
    public void shouldParseEnumField()
    {
        final Field msgType = field("MsgType");

        assertNotNull("Hasn't found MsgType", msgType);
        assertEquals("MsgType", msgType.name());
        assertEquals(35, msgType.number());
        assertEquals(STRING, msgType.type());

        final List<Value> values = msgType.values();

        assertEquals(new Value('0', "HEARTBEAT"), values.get(0));
        assertEquals(new Value('8', "EXECUTION_REPORT"), values.get(1));
        assertEquals(new Value('D', "ORDER_SINGLE"), values.get(2));
    }

    @Test
    public void shouldParseAllEnum()
    {
        assertEquals(9, countEnumFields());
    }

    @Test
    public void shouldParseSimpleMessage()
    {
        final Message heartbeat = dictionary.messages().get(0);

        assertEquals("Heartbeat", heartbeat.name());
        assertEquals('0', heartbeat.type());
        assertEquals(ADMIN, heartbeat.category());

        final Field field = heartbeat.optionalFields().get(0);

        assertEquals("TestReqID", field.name());
        assertEquals(112, field.number());
        assertFalse(field.isEnum());
        assertEquals(STRING, field.type());
    }

    @Test
    public void shouldParseAllMessages()
    {
        assertEquals(3, dictionary.messages().size());
    }

    @Test
    public void messagesShouldHaveCommonFields()
    {
        final Message heartbeat = dictionary.messages().get(0);

        final List<Field> fields = heartbeat.requiredFields();

        assertEquals("BeginString", fields.get(0).name());
        assertEquals("BodyLength", fields.get(1).name());
        assertEquals("MsgType", fields.get(2).name());
        assertEquals("CheckSum", fields.get(3).name());
    }

    @Test
    public void shouldParseGroups()
    {
        final Message newOrderSingle = dictionary.messages().get(1);
        final List<Group> groups = newOrderSingle.groups();
        assertThat(groups, hasSize(1));

        final Group noTradingSessions = groups.get(0);

        assertEquals("NoTradingSessions", noTradingSessions.name());
        assertFalse(noTradingSessions.required());
        assertThat(noTradingSessions.requiredFields(), hasSize(0));
        assertThat(noTradingSessions.optionalFields(), contains(field("TradingSessionID")));
    }

    // TODO: Components

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
