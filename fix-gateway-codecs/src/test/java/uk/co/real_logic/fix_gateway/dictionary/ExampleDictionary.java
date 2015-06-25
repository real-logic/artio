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
package uk.co.real_logic.fix_gateway.dictionary;

import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.ENCODER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.PARENT_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Category.ADMIN;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Field.registerField;

public final class ExampleDictionary
{

    public static final String NO_EG_GROUP = "NoEgGroup";
    public static final String EG_COMPONENT = "EgComponent";

    public static final String EG_ENUM = PARENT_PACKAGE + "." + "EgEnum";
    public static final String OTHER_ENUM = PARENT_PACKAGE + "." + "OtherEnum";
    public static final String STRING_ENUM = PARENT_PACKAGE + "." + "stringEnum";

    public static final String TEST_PACKAGE = ENCODER_PACKAGE + ".test";

    public static final String HEARTBEAT_ENCODER = TEST_PACKAGE + ".HeartbeatEncoder";
    public static final String COMPONENT_ENCODER = TEST_PACKAGE + "." + EG_COMPONENT + "Encoder";
    public static final String HEADER_ENCODER = TEST_PACKAGE + ".HeaderEncoder";

    public static final String HEARTBEAT_DECODER = TEST_PACKAGE + ".HeartbeatDecoder";
    public static final String HEADER_DECODER = TEST_PACKAGE + ".HeaderDecoder";
    public static final String COMPONENT_DECODER = TEST_PACKAGE + "." + EG_COMPONENT + "Decoder";

    public static final String PRINTER = TEST_PACKAGE + ".PrinterImpl";

    public static final String ABC = "abc";
    public static final byte[] VALUE_IN_BYTES = {97, 98, 99};
    public static final String TEST_REQ_ID = "testReqID";
    public static final String INT_FIELD = "intField";
    public static final String FLOAT_FIELD = "floatField";
    public static final String BOOLEAN_FIELD = "booleanField";
    public static final String DATA_FIELD = "dataField";
    public static final String TEST_REQ_ID_LENGTH = "testReqIDLength";

    public static final String HAS_TEST_REQ_ID = "hasTestReqID";
    public static final String HAS_BOOLEAN_FIELD = "hasBooleanField";
    public static final String HAS_DATA_FIELD = "hasDataField";

    public static final String MSG_TYPE = "msgType";
    public static final String BODY_LENGTH = "bodyLength";

    public static final int HEARTBEAT_TYPE = '0';

    public static final Dictionary FIELD_EXAMPLE;

    public static final Dictionary MESSAGE_EXAMPLE;

    public static final String HEADER_TO_STRING =
        "  \"header\": {\n" +
        "    \"MsgType\": \"Header\",\n" +
        "    \"BeginString\": \"FIX.4.4\",\n" +
        "    \"BodyLength\": \"%s\",\n" +
        "    \"MsgType\": \"0\",\n" +
        "  }\n";

    public static final String STRING_ENCODED_MESSAGE_SUFFIX =
        "  \"OnBehalfOfCompID\": \"abc\",\n" +
        "  \"TestReqID\": \"abc\",\n" +
        "  \"IntField\": \"2\",\n" +
        "  \"FloatField\": \"1.1\",\n" +
        "  \"BooleanField\": \"true\",\n" +
        "  \"DataField\": \"[49, 50, 51]\",\n";

    public static final String STRING_FOR_GROUP =
        "  \"EgGroup\": [\n" +
        "  {\n" +
        "    \"MsgType\": \"EgGroup\",\n" +
        "    \"GroupField\": \"1\",\n" +
        "  },\n" +
        "  {\n" +
        "    \"MsgType\": \"EgGroup\",\n" +
        "    \"GroupField\": \"2\",\n" +
        "  }\n" +
        "  ]";

    public static final String STRING_ENCODED_MESSAGE_EXAMPLE =
        "{\n" +
        "  \"MsgType\": \"Heartbeat\",\n" +
        String.format(HEADER_TO_STRING, 49) +
        STRING_ENCODED_MESSAGE_SUFFIX;

    public static final String STRING_NO_OPTIONAL_MESSAGE_SUFFIX =
        "  \"OnBehalfOfCompID\": \"abc\",\n" +
            "  \"IntField\": \"2\",\n" +
            "  \"FloatField\": \"1.1\",\n";

    public static final String STRING_NO_OPTIONAL_MESSAGE_EXAMPLE =
        "{\n" +
            "  \"MsgType\": \"Heartbeat\",\n" +
            String.format(HEADER_TO_STRING, 27) +
            STRING_NO_OPTIONAL_MESSAGE_SUFFIX;

    public static final String COMPONENT_TO_STRING =
        "  \"EgComponent\":  {\n" +
        "    \"MsgType\": \"EgComponent\",\n" +
        "    \"ComponentField\": \"2\",\n" +
        "  }";

    public static final String ENCODED_MESSAGE_EXAMPLE =
        "8=FIX.4.4\0019=0049\00135=0\001115=abc\001112=abc\001116=2\001117=1.1\001118=Y\001119=123\00110=061\001";

    public static final String NO_OPTIONAL_MESSAGE_EXAMPLE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=2\001117=1.1\00110=161\001";

    public static final String DERIVED_FIELDS_EXAMPLE =
            "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=2\001117=1.1\00110=161\001";

    public static final String SHORTER_STRING_EXAMPLE =
        "8=FIX.4.4\0019=0026\00135=0\001115=ab\001116=2\001117=1.1\00110=061\001";

    public static final String REPEATING_GROUP_EXAMPLE =
        "8=FIX.4.4\0019=0045\00135=0\001115=abc\001116=2\001117=1.1\001120=2\001121=1\001121=2\00110=171\001";

    public static final String NESTED_GROUP_EXAMPLE =
        "8=FIX.4.4\0019=0051\00135=0\001115=abc\001116=2\001117=1.1\001120=1\001121=1\001122=1\001123=1\00110=172\001";

    public static final String COMPONENT_MESSAGE_EXAMPLE =
        "8=FIX.4.4\0019=0033\00135=0\001115=abc\001116=2\001117=1.1\001124=2\00110=165\001";

    public static final int TEST_REQ_ID_TAG = 112;

    static
    {
        FIELD_EXAMPLE = buildFieldExample();
        MESSAGE_EXAMPLE = buildMessageExample();
    }

    private static Dictionary buildMessageExample()
    {
        final Map<String, Field> messageEgFields = new HashMap<>();

        final Field beginString = registerField(messageEgFields, 8, "BeginString", Type.STRING);
        final Field bodyLength = registerField(messageEgFields, 9, "BodyLength", Type.INT);
        final Field msgType = registerField(messageEgFields, 35, "MsgType", Type.STRING);

        final Field checkSum = registerField(messageEgFields, 10, "CheckSum", Type.STRING);

        final Field onBehalfOfCompID = registerField(messageEgFields, 115, "OnBehalfOfCompID", Type.STRING);
        final Field testReqID = registerField(messageEgFields, TEST_REQ_ID_TAG, "TestReqID", Type.STRING);
        final Field intField = registerField(messageEgFields, 116, "IntField", Type.LENGTH);
        final Field floatField = registerField(messageEgFields, 117, "FloatField", Type.PRICE);
        final Field booleanField = registerField(messageEgFields, 118, "BooleanField", Type.BOOLEAN);
        final Field dataField = registerField(messageEgFields, 119, "DataField", Type.DATA);

        final Group nestedGroup = Group.of(registerField(messageEgFields, 122, "NoNestedGroup", Type.INT));
        nestedGroup.optionalEntry(registerField(messageEgFields, 123, "NestedField", Type.INT));

        final Group egGroup = Group.of(registerField(messageEgFields, 120, NO_EG_GROUP, Type.INT));
        egGroup.optionalEntry(registerField(messageEgFields, 121, "GroupField", Type.INT));
        egGroup.optionalEntry(nestedGroup);

        final Component egComponent = new Component(EG_COMPONENT);
        egComponent.optionalEntry(registerField(messageEgFields, 124, "ComponentField", Type.INT));

        final Message heartbeat = new Message("Heartbeat", HEARTBEAT_TYPE, ADMIN);
        heartbeat.requiredEntry(onBehalfOfCompID);
        heartbeat.optionalEntry(testReqID);
        heartbeat.requiredEntry(intField);
        heartbeat.requiredEntry(floatField);
        heartbeat.optionalEntry(booleanField);
        heartbeat.optionalEntry(dataField);
        heartbeat.optionalEntry(egGroup);
        heartbeat.requiredEntry(egComponent);

        final Component header = new Component("Header");
        header.requiredEntry(beginString)
              .requiredEntry(bodyLength)
              .requiredEntry(msgType);

        final Component trailer = new Component("Trailer");
        trailer.requiredEntry(checkSum);

        final List<Message> messages = singletonList(heartbeat);

        final Map<String, Component> components = new HashMap<>();
        components.put(EG_COMPONENT, egComponent);

        return new Dictionary(messages, messageEgFields, components, header, trailer, 4, 4);
    }

    private static Dictionary buildFieldExample()
    {
        final Field egEnum = new Field(123, "EgEnum", Type.CHAR)
            .addValue("a", "AnEntry")
            .addValue("b", "AnotherEntry");

        final Field otherEnum = new Field(124, "OtherEnum", Type.INT)
            .addValue("1", "AnEntry")
            .addValue("12", "AnotherEntry");

        final Field stringEnum = new Field(126, "stringEnum", Type.STRING)
            .addValue("0", "_0")
            .addValue("A", "_A")
            .addValue("AA", "_AAA");

        final Map<String, Field> fieldEgFields = new HashMap<>();
        fieldEgFields.put("EgEnum", egEnum);
        fieldEgFields.put("OtherEnum", otherEnum);
        fieldEgFields.put("stringEnum", stringEnum);
        fieldEgFields.put("egNotEnum", new Field(125, "EgNotEnum", Type.CHAR));

        return new Dictionary(emptyList(), fieldEgFields, emptyMap(), null, null, 4, 4);
    }
}
