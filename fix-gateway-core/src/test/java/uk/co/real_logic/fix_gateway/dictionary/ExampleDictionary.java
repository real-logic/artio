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

import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.BUILDER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.PARENT_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Category.ADMIN;

public final class ExampleDictionary
{
    public static final String EG_ENUM = PARENT_PACKAGE + "." + "EgEnum";

    public static final String HEARTBEAT = BUILDER_PACKAGE + "." + "Heartbeat";

    public static final DataDictionary FIELD_EXAMPLE;

    public static final DataDictionary MESSAGE_EXAMPLE;

    /*"8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
            "52=20090323-15:40:29\00156=CCG\001115=XYZ\00111=NF 0542/03232009\00154=1\00138=100\00155=CVS\00140=1" +
            "\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=194\001"*/

    // Just the message body - no header and no checksum
    public static final String ENCODED_MESSAGE_EXAMPLE = "115=abc\001112=abc\001116=2\001117=1.1\001";

    static
    {
        final Field egEnum = new Field(123, "EgEnum", Type.CHAR);
        egEnum.addValue('a', "AnEntry");
        egEnum.addValue('b', "AnotherEntry");

        final Map<String, Field> fieldEgFields = new HashMap<>();
        fieldEgFields.put("EgEnum", egEnum);
        fieldEgFields.put("egNotEnum", new Field(123, "EgNotEnum", Type.CHAR));

        FIELD_EXAMPLE = new DataDictionary(emptyList(), fieldEgFields, emptyMap());

        final Field onBehalfOfCompID = new Field(115, "OnBehalfOfCompID", Type.STRING);
        final Field testReqID = new Field(112, "TestReqID", Type.STRING);
        final Field intField = new Field(116, "IntField", Type.LENGTH);
        final Field floatField = new Field(117, "FloatField", Type.PRICE);

        final Message heartbeat = new Message("Heartbeat", '0', ADMIN);
        heartbeat.requiredEntry(onBehalfOfCompID);
        heartbeat.optionalEntry(testReqID);
        heartbeat.requiredEntry(intField);
        heartbeat.requiredEntry(floatField);

        final Map<String, Field> messageEgFields = new HashMap<>();
        messageEgFields.put("OnBehalfOfCompID", onBehalfOfCompID);
        messageEgFields.put("TestReqID", testReqID);
        messageEgFields.put("IntField", intField);
        messageEgFields.put("FloatField", floatField);

        MESSAGE_EXAMPLE = new DataDictionary(singletonList(heartbeat), messageEgFields, emptyMap());
    }
}
