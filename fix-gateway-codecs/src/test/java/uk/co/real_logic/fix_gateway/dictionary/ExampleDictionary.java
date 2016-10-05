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

import uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.ENCODER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.PARENT_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Category.ADMIN;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type.INT;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Field.registerField;

public final class ExampleDictionary
{

    public static final String NO_EG_GROUP = "NoEgGroup";
    public static final String NO_COMPONENT_GROUP = "NoComponentGroup";
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
    public static final String OTHER_MESSAGE_DECODER = TEST_PACKAGE + ".OtherMessageDecoder";
    public static final String OTHER_MESSAGE_ENCODER = TEST_PACKAGE + ".OtherMessageEncoder";

    public static final String PRINTER = TEST_PACKAGE + ".PrinterImpl";

    public static final String ABC = "abc";
    public static final byte[] VALUE_IN_BYTES = {97, 98, 99};
    public static final String TEST_REQ_ID = "testReqID";
    public static final String INT_FIELD = "intField";
    public static final String FLOAT_FIELD = "floatField";
    public static final String BOOLEAN_FIELD = "booleanField";
    public static final String DATA_FIELD = "dataField";
    public static final String TEST_REQ_ID_LENGTH = "testReqIDLength";
    public static final String SOME_TIME_FIELD = "someTimeField";
    public static final String COMPONENT_FIELD = "componentField";

    public static final String HAS_TEST_REQ_ID = "hasTestReqID";
    public static final String HAS_BOOLEAN_FIELD = "hasBooleanField";
    public static final String HAS_DATA_FIELD = "hasDataField";
    public static final String HAS_COMPONENT_FIELD = "hasComponentField";

    public static final String MSG_TYPE = "msgType";
    public static final String BODY_LENGTH = "bodyLength";

    public static final int HEARTBEAT_TYPE = '0';

    public static final Dictionary FIELD_EXAMPLE;

    public static final Dictionary MESSAGE_EXAMPLE;

    public static final String HEADER_TO_STRING =
        "  \"header\": {\n" +
        "    \"MessageName\": \"Header\",\n" +
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
        "  \"DataField\": \"[49, 50, 51]\",\n" +
        "  \"SomeTimeField\": \"19700101-00:00:00.001\"";

    public static final String STRING_FOR_GROUP =
        "  \"EgGroupGroup\": [\n" +
        "  {\n" +
        "    \"MessageName\": \"EgGroupGroup\",\n" +
        "    \"GroupField\": \"1\",\n" +
        "  },\n" +
        "  {\n" +
        "    \"MessageName\": \"EgGroupGroup\",\n" +
        "    \"GroupField\": \"2\",\n" +
        "  }\n" +
        "  ]";

    public static final String STRING_ENCODED_MESSAGE_EXAMPLE =
        "{\n" +
        "  \"MessageName\": \"Heartbeat\",\n" +
        String.format(HEADER_TO_STRING, 75) +
        STRING_ENCODED_MESSAGE_SUFFIX;

    public static final String STRING_NO_OPTIONAL_MESSAGE_SUFFIX =
        "  \"OnBehalfOfCompID\": \"abc\",\n" +
        "  \"IntField\": \"2\",\n" +
        "  \"FloatField\": \"1.1\",\n" +
        "  \"SomeTimeField\": \"19700101-00:00:00.001\"";

    public static final String STRING_RESET_SUFFIX =
        "  \"OnBehalfOfCompID\": \"\",\n" +
        "  \"IntField\": \"-1\",\n" +
        "  \"SomeTimeField\": \"\"";

    public static final String STRING_NO_OPTIONAL_MESSAGE_EXAMPLE =
        "{\n" +
            "  \"MessageName\": \"Heartbeat\",\n" +
            String.format(HEADER_TO_STRING, 53) +
            STRING_NO_OPTIONAL_MESSAGE_SUFFIX;

    public static final String COMPONENT_TO_STRING =
        "  \"EgComponent\":  {\n" +
        "    \"MessageName\": \"EgComponent\",\n" +
        "    \"ComponentField\": \"2\",\n" +
        "    \"ComponentGroupGroup\": [\n" +
        "    {\n" +
        "      \"MessageName\": \"ComponentGroupGroup\",\n" +
        "      \"ComponentGroupField\": \"1\",\n" +
        "    },\n" +
        "    {\n" +
        "      \"MessageName\": \"ComponentGroupGroup\",\n" +
        "      \"ComponentGroupField\": \"2\",\n" +
        "    }\n" +
        "    ]\n" +
        "  }";

    public static final String ENCODED_MESSAGE =
        "8=FIX.4.4\0019=0075\00135=0\001115=abc\001112=abc\001116=2\001117=1.1" +
            "\001118=Y\001119=123\001127=19700101-00:00:00.001\00110=039\001";

    public static final String NO_OPTIONAL_MESSAGE =
        "8=FIX.4.4\0019=0053\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=139\001";

    public static final String MISSING_REQUIRED_FIELDS_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=161\001";

    public static final String MISSING_EVERYTHING =
        "8=FIX.4.4\0019=0027\00110=161\001";

    public static final String INVALID_TAG_NUMBER_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\0019999=9999\00110=161\001";

    public static final String TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\00199=9999\00110=161\001";

    public static final String TAG_SPECIFIED_WITHOUT_A_VALUE_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=161\001";

    public static final String TAG_SPECIFIED_WHERE_INT_VALUE_IS_INCORRECT_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=10\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=161\001";

    public static final String TAG_SPECIFIED_WHERE_STRING_VALUE_IS_INCORRECT_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=ZZZZ\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=161\001";

    public static final String TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE =
        "35=0\0018=FIX.4.4\0019=0027\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=161\001";

    public static final byte[] TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES =
        TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE.getBytes(US_ASCII);

    public static final String TAG_APPEARS_MORE_THAN_ONCE_MESSAGE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=2\001116=1\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=161\001";

    public static final String DERIVED_FIELDS_MESSAGE =
            "8=FIX.4.4\0019=0053\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
                "\00110=139\001";

    public static final String SHORTER_STRING_MESSAGE =
        "8=FIX.4.4\0019=0052\00135=0\001115=ab\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\00110=039\001";

    public static final String REPEATING_GROUP_MESSAGE =
        "8=FIX.4.4\0019=0071\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\001120=2\001121=1\001121=2\00110=149\001";

    public static final String SINGLE_REPEATING_GROUP_MESSAGE =
        "8=FIX.4.4\0019=0065\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\001120=1\001121=2\00110=148\001";

    public static final String ZERO_REPEATING_GROUP_MESSAGE =
        "8=FIX.4.4\0019=0059\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\001120=0\00110=146\001";

    public static final String NESTED_GROUP_MESSAGE =
        "8=FIX.4.4\0019=0077\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\001120=1\001121=1\001122=1\001123=1\00110=159\001";

    public static final String COMPONENT_MESSAGE =
        "8=FIX.4.4\0019=0077\00135=0\001115=abc\001116=2\001117=1.1\001127=19700101-00:00:00.001" +
            "\001124=2\001130=2\001131=1\001131=2\00110=165\001";

    public static final String SHORT_TIMESTAMP_MESSAGE =
        "8=FIX.4.4\0019=0049\00135=0\001115=abc\001116=2\001117=1.1" +
            "\001127=19700101-00:00:00\00110=209\001";

    public static final int TEST_REQ_ID_TAG = 112;

    public static final String OTHER_MESSAGE_TYPE = "AB";
    public static final byte[] OTHER_MESSAGE_TYPE_BYTES = OTHER_MESSAGE_TYPE.getBytes(US_ASCII);
    public static final int OTHER_MESSAGE_TYPE_PACKED = GenerationUtil.packMessageType(OTHER_MESSAGE_TYPE);

    static
    {
        FIELD_EXAMPLE = buildFieldExample();
        MESSAGE_EXAMPLE = buildMessageExample();
    }

    private static Dictionary buildMessageExample()
    {
        final Map<String, Field> messageEgFields = new HashMap<>();

        final Field beginString = registerField(messageEgFields, 8, "BeginString", Type.STRING);
        final Field bodyLength = registerField(messageEgFields, 9, "BodyLength", INT);
        final Field msgType = registerField(messageEgFields, 35, "MsgType", Type.STRING);

        final Field checkSum = registerField(messageEgFields, 10, "CheckSum", Type.STRING);

        final Field onBehalfOfCompID = registerField(messageEgFields, 115, "OnBehalfOfCompID", Type.STRING)
            .addValue("abc", "abc")
            .addValue("def", "def");

        final Field testReqID = registerField(messageEgFields, TEST_REQ_ID_TAG, "TestReqID", Type.STRING);
        final Field intField = registerField(messageEgFields, 116, "IntField", Type.LENGTH)
            .addValue("1", "ONE")
            .addValue("2", "TWO");

        final Field floatField = registerField(messageEgFields, 117, "FloatField", Type.PRICE);
        final Field booleanField = registerField(messageEgFields, 118, "BooleanField", Type.BOOLEAN);
        final Field dataField = registerField(messageEgFields, 119, "DataField", Type.DATA);
        final Field someTime = registerField(messageEgFields, 127,  "SomeTimeField", Type.UTCTIMESTAMP);
        final Field charField = registerField(messageEgFields, 128,  "CharField", Type.CHAR);
        final Field dayOfMonthField = registerField(messageEgFields, 129,  "DayOfMonthField", Type.DAYOFMONTH);

        final Group nestedGroup = Group.of(registerField(messageEgFields, 122, "NoNestedGroup", INT));
        nestedGroup.optionalEntry(registerField(messageEgFields, 123, "NestedField", INT));

        final Group egGroup = Group.of(registerField(messageEgFields, 120, NO_EG_GROUP, INT));
        egGroup.optionalEntry(registerField(messageEgFields, 121, "GroupField", INT));
        egGroup.optionalEntry(nestedGroup);

        final Group componentGroup = Group.of(registerField(messageEgFields, 130, NO_COMPONENT_GROUP, INT));
        componentGroup.optionalEntry(registerField(messageEgFields, 131, "ComponentGroupField", INT));

        final Component egComponent = new Component(EG_COMPONENT);
        egComponent.optionalEntry(registerField(messageEgFields, 124, "ComponentField", INT));
        egComponent.optionalEntry(componentGroup);

        final Message heartbeat = new Message("Heartbeat", "0", ADMIN);
        heartbeat.requiredEntry(onBehalfOfCompID);
        heartbeat.optionalEntry(testReqID);
        heartbeat.requiredEntry(intField);
        heartbeat.requiredEntry(floatField);
        heartbeat.optionalEntry(booleanField);
        heartbeat.optionalEntry(dataField);
        heartbeat.optionalEntry(charField);
        heartbeat.optionalEntry(dayOfMonthField);
        heartbeat.requiredEntry(someTime);
        heartbeat.optionalEntry(egGroup);
        heartbeat.requiredEntry(egComponent);

        final Component header = new Component("Header");
        header.requiredEntry(beginString)
              .requiredEntry(bodyLength)
              .requiredEntry(msgType);

        final Component trailer = new Component("Trailer");
        trailer.requiredEntry(checkSum);

        final Message otherMessage = new Message("OtherMessage", OTHER_MESSAGE_TYPE, ADMIN);
        otherMessage.requiredEntry(registerField(messageEgFields, 99, "OtherField", INT));

        final List<Message> messages = asList(heartbeat, otherMessage);

        final Map<String, Component> components = new HashMap<>();
        components.put(EG_COMPONENT, egComponent);

        return new Dictionary(messages, messageEgFields, components, header, trailer, 4, 4);
    }

    private static Dictionary buildFieldExample()
    {
        final Field egEnum = new Field(123, "EgEnum", Type.CHAR)
            .addValue("a", "AnEntry")
            .addValue("b", "AnotherEntry");

        final Field otherEnum = new Field(124, "OtherEnum", INT)
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
