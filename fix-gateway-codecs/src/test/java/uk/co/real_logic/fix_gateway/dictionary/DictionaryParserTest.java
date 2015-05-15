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

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Value;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Category.ADMIN;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type.STRING;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.hasProperty;

@SuppressWarnings("unchecked")
public class DictionaryParserTest
{
    private static final String EXAMPLE_FILE = "example_dictionary.xml";

    private Dictionary dictionary;

    @Before
    public void setUp() throws Exception
    {
        dictionary = parseExample();
    }

    @Test
    public void shouldParseVersionNumbers()
    {
        assertEquals(7, dictionary.majorVersion());
        assertEquals(2, dictionary.minorVersion());
    }

    @Test
    public void shouldParseExampleDictionary()
    {
        assertNotNull("Missing dictionary", dictionary);
        assertNotNull("Missing messages", dictionary.messages());
        assertNotNull("Missing fields", dictionary.fields());
        assertNotNull("Missing components", dictionary.components());
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
    public void shouldParseTestReqID()
    {
        final Field field = field("TestReqID");
        assertEquals("TestReqID", field.name());
        assertEquals(112, field.number());
        assertFalse(field.isEnum());
        assertEquals(STRING, field.type());
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

        assertEquals(new Value("0", "HEARTBEAT"), values.get(0));
        assertEquals(new Value("8", "EXECUTION_REPORT"), values.get(1));
        assertEquals(new Value("D", "ORDER_SINGLE"), values.get(2));
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

        final Entry entry = heartbeat.entries().get(0);
        assertThat(entry, isRequiredField("TestReqID", false));
    }

    @Test
    public void shouldParseAllMessages()
    {
        assertEquals(3, dictionary.messages().size());
    }

    @Test
    public void shouldHaveHeader()
    {
        final List<Entry> entries = dictionary.header().entries();

        assertThat(entries, hasSize(3));
        assertThat(entries, hasItem(isRequiredField("BeginString", true)));
        assertThat(entries, hasItem(isRequiredField("BodyLength", true)));
        assertThat(entries, hasItem(isRequiredField("MsgType", true)));
    }

    @Test
    public void shouldHaveTrailer()
    {
        final List<Entry> entries = dictionary.trailer().entries();

        assertThat(entries, hasItems(isRequiredField("CheckSum", true)));
    }

    @Test
    public void shouldParseGroups()
    {
        final Message newOrderSingle = newOrderSingle();
        final List<Entry> entries = newOrderSingle.entries();

        assertThat(entries,
            hasItem(withElement(isGroup("TradingSessions",
                    withEntries(hasItems(isField("TradingSessionID")))))));
    }

    @Test
    public void shouldParseComponents()
    {
        final Message newOrderSingle = newOrderSingle();

        assertThat(newOrderSingle.entries(),
            hasItem(isRequiredComponent("Members")));
    }

    @Test
    public void shouldParseNestedGroups()
    {
        final Component members = component("Members");
        final Group noMemberIDs = (Group)members.entries().get(0).element();

        final Entry noMemberSubIDs = noMemberIDs.entries().get(1);
        assertFalse(noMemberSubIDs.required());
        assertThat(noMemberSubIDs.element(), instanceOf(Group.class));
    }

    private Component component(final String name)
    {
        return dictionary.components().get(name);
    }

    private Message newOrderSingle()
    {
        return dictionary.messages().get(1);
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

    private Dictionary parseExample() throws Exception
    {
        return new DictionaryParser().parse(DictionaryParserTest.class.getResourceAsStream(EXAMPLE_FILE));
    }

    private <T> Matcher<T> withElement(final Matcher<?> valueMatcher)
    {
        return hasProperty("element", valueMatcher);
    }

    private <T> Matcher<T> withName(final Matcher<?> valueMatcher)
    {
        return hasProperty("name", valueMatcher);
    }

    private <T> Matcher<T> isRequired(final boolean required)
    {
        return hasProperty("required", equalTo(required));
    }

    private <T> Matcher<T> withEntries(final Matcher<?> valueMatcher)
    {
        return hasProperty("entries", valueMatcher);
    }

    private <T> Matcher<T> isField(final String name)
    {
        return withElement(equalTo(field(name)));
    }

    private <T> Matcher<T> isRequiredField(final String name, final boolean required)
    {
        return allOf(isRequired(required), isField(name));
    }

    private <T> Matcher<T> isComponent(final String name)
    {
        return withElement(equalTo(component(name)));
    }

    private <T> Matcher<T> isRequiredComponent(final String name)
    {
        return allOf(isRequired(false), isComponent(name));
    }

    private <T> Matcher<T> isGroup(final String name, final Matcher<T> valueMatcher)
    {
        return allOf(instanceOf(Group.class), withName(equalTo(name)), valueMatcher);
    }
}
