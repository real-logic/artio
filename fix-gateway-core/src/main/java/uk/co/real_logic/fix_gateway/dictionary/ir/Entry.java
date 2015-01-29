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
package uk.co.real_logic.fix_gateway.dictionary.ir;

import java.util.ArrayList;
import java.util.List;

/**
 * An entry is either a group or a message.
 */
public abstract class Entry
{
    private final String name;
    private final List<Field> requiredFields;
    private final List<Field> optionalFields;

    protected Entry(final String name)
    {
        this.name = name;
        this.requiredFields = new ArrayList<>();
        this.optionalFields = new ArrayList<>();
    }

    public String name()
    {
        return name;
    }

    public List<Field> requiredFields()
    {
        return requiredFields;
    }

    public List<Field> optionalFields()
    {
        return optionalFields;
    }
}
