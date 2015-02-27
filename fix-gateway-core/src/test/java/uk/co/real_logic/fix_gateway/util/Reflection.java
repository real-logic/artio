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
package uk.co.real_logic.fix_gateway.util;

import uk.co.real_logic.fix_gateway.fields.DecimalFloat;

public final class Reflection
{
    private Reflection()
    {
    }

    public static void setInt(final Object object, final String setter, final int value) throws Exception
    {
        set(object, setter, int.class, value);
    }

    public static void setFloat(final Object object, final String setter, final DecimalFloat value) throws Exception
    {
        set(object, setter, DecimalFloat.class, value);
    }

    public static void setCharSequence(final Object object, final String setter, final CharSequence value) throws Exception
    {
        set(object, setter, CharSequence.class, value);
    }

    private static void set(
        final Object object,
        final String setter,
        final Class<?> type,
        final Object value) throws Exception
    {
        object.getClass()
              .getMethod(setter, type)
              .invoke(object, value);
    }
}
