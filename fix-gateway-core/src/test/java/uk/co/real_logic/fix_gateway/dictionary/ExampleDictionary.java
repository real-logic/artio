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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.PARENT_PACKAGE;

public final class ExampleDictionary
{
    public static final String EG_ENUM = PARENT_PACKAGE + "." + "EgEnum";

    public static final DataDictionary FIELD_EXAMPLE;

    static
    {
        final Field egEnum = new Field(123, "EgEnum", Field.Type.CHAR);
        egEnum.addValue('a', "AnEntry");
        egEnum.addValue('b', "AnotherEntry");

        final Map<String, Field> fields = new HashMap<>();
        fields.put("EgEnum", egEnum);
        fields.put("egNotEnum", new Field(123, "EgNotEnum", Field.Type.CHAR));

        FIELD_EXAMPLE = new DataDictionary(Collections.emptyList(), fields, Collections.emptyMap());
    }
}
