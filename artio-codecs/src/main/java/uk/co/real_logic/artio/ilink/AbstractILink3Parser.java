/*
 * Copyright 2020 Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.ilink;

import org.agrona.DirectBuffer;

import java.lang.reflect.InvocationTargetException;

public abstract class AbstractILink3Parser
{
    public static final int ILINK_MESSAGE_HEADER_LENGTH = 8;
    public static final int BOOLEAN_FLAG_TRUE = 1;

    public static AbstractILink3Parser make(final ILink3EndpointHandler session)
    {
        try
        {
            final Class<?> cls = Class.forName("uk.co.real_logic.artio.ilink.ILink3Parser");
            return (AbstractILink3Parser)cls.getConstructor(ILink3EndpointHandler.class).newInstance(session);
        }
        catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
            IllegalAccessException | InvocationTargetException e)
        {
            return null;
        }
    }

    public abstract long onMessage(DirectBuffer buffer, int offset);

    public abstract int templateId(DirectBuffer buffer, int offset);
}
