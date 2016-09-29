/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;

import java.lang.reflect.Field;

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

    public static void setBoolean(final Object object, final String setter, final boolean value) throws Exception
    {
        set(object, setter, boolean.class, value);
    }

    public static void setByteArray(final Object object, final String setter, final byte[] value) throws Exception
    {
        set(object, setter, byte[].class, value);
    }

    private static void set(
        final Object object,
        final String setterName,
        final Class<?> type,
        final Object value) throws Exception
    {
        object.getClass()
              .getMethod(setterName, type)
              .invoke(object, value);
    }

    public static void setField(
            final Object object,
            final String fieldName,
            final Object value) throws Exception
    {
        field(object, fieldName).set(object, value);
    }

    public static Object get(final Object value, final String name) throws Exception
    {
        return value.getClass()
                    .getMethod(name)
                    .invoke(value);
    }

    public static Object getField(final Object object, final String fieldName) throws Exception
    {
        return field(object, fieldName).get(object);
    }

    public static Field field(Object object, String fieldName) throws NoSuchFieldException
    {
        final Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    public static Object call(final Object value, final String methodName) throws Exception
    {
        return value.getClass()
                    .getMethod(methodName)
                    .invoke(value);
    }

    public static void reset(final Encoder encoder) throws Exception
    {
        call(encoder, "reset");
    }

    public static Object next(final Object stub) throws Exception
    {
        return call(stub, "next");
    }

    public static Object getEgGroup(final Object stub) throws Exception
    {
        return get(stub, "egGroupGroup");
    }

    public static Object getComponentGroup(final Object stub) throws Exception
    {
        return get(stub, "componentGroupGroup");
    }

    public static Object getNestedGroup(final Object group) throws Exception
    {
        return get(group, "nestedGroupGroup");
    }

    public static Object getEgComponent(final Object object) throws Exception
    {
        return get(object, "egComponent");
    }
}
