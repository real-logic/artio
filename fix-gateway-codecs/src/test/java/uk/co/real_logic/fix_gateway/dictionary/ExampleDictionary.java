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

import uk.co.real_logic.fix_gateway.dictionary.ir.Component;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.ENCODER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.PARENT_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Category.ADMIN;

public final class ExampleDictionary
{
    public static final String EG_ENUM = PARENT_PACKAGE + "." + "EgEnum";
    public static final String TEST_PACKAGE = ENCODER_PACKAGE + ".test";

    public static final String HEARTBEAT_ENCODER = TEST_PACKAGE + ".HeartbeatEncoder";
    public static final String HEADER_ENCODER = TEST_PACKAGE + ".HeaderEncoder";

    public static final String HEARTBEAT_DECODER = TEST_PACKAGE + ".HeartbeatDecoder";
    public static final String HEADER_DECODER = TEST_PACKAGE + ".HeaderDecoder";

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

    public static final DataDictionary FIELD_EXAMPLE;

    public static final DataDictionary MESSAGE_EXAMPLE;

    public static final String ENCODED_MESSAGE_EXAMPLE =
        "8=abc\0019=0051\00135=abc\001115=abc\001112=abc\001116=2\001117=1.1\001118=Y\001119=123\00110=108\001";

    public static final String NO_OPTIONAL_MESSAGE_EXAMPLE =
        "8=abc\0019=0029\00135=abc\001115=abc\001116=2\001117=1.1\00110=212\001";

    public static final String DERIVED_FIELDS_EXAMPLE =
        "8=FIX.4.4\0019=0027\00135=0\001115=abc\001116=2\001117=1.1\00110=222\001";

    static
    {
        final Field egEnum = new Field(123, "EgEnum", Type.CHAR);
        egEnum.addValue('a', "AnEntry");
        egEnum.addValue('b', "AnotherEntry");

        final Map<String, Field> fieldEgFields = new HashMap<>();
        fieldEgFields.put("EgEnum", egEnum);
        fieldEgFields.put("egNotEnum", new Field(123, "EgNotEnum", Type.CHAR));

        FIELD_EXAMPLE = new DataDictionary(emptyList(), fieldEgFields, emptyMap(), null, null, 4, 4);

        final Map<String, Field> messageEgFields = new HashMap<>();

        final Field beginString = Field.register(messageEgFields, 8, "BeginString", Type.STRING);
        final Field bodyLength = Field.register(messageEgFields, 9, "BodyLength", Type.INT);
        final Field msgType = Field.register(messageEgFields, 35, "MsgType", Type.STRING);

        final Field checkSum = Field.register(messageEgFields, 10, "CheckSum", Type.STRING);

        final Field onBehalfOfCompID = Field.register(messageEgFields, 115, "OnBehalfOfCompID", Type.STRING);
        final Field testReqID = Field.register(messageEgFields, 112, "TestReqID", Type.STRING);
        final Field intField = Field.register(messageEgFields, 116, "IntField", Type.LENGTH);
        final Field floatField = Field.register(messageEgFields, 117, "FloatField", Type.PRICE);
        final Field booleanField = Field.register(messageEgFields, 118, "BooleanField", Type.BOOLEAN);
        final Field dataField = Field.register(messageEgFields, 119, "DataField", Type.DATA);

        final Message heartbeat = new Message("Heartbeat", '0', ADMIN);
        heartbeat.requiredEntry(onBehalfOfCompID);
        heartbeat.optionalEntry(testReqID);
        heartbeat.requiredEntry(intField);
        heartbeat.requiredEntry(floatField);
        heartbeat.optionalEntry(booleanField);
        heartbeat.optionalEntry(dataField);

        final Component header = new Component("Header");
        header.requiredEntry(beginString)
              .requiredEntry(bodyLength)
              .requiredEntry(msgType);

        final Component trailer = new Component("Trailer");
        trailer.requiredEntry(checkSum);

        final List<Message> messages = singletonList(heartbeat);

        MESSAGE_EXAMPLE = new DataDictionary(messages, messageEgFields, emptyMap(), header, trailer, 4, 4);
    }
}
