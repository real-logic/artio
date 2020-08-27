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
import org.agrona.ErrorHandler;

import java.lang.reflect.InvocationTargetException;

public abstract class AbstractILink3Offsets
{
    public static final int MISSING_OFFSET = -1;

    public static AbstractILink3Offsets make(final ErrorHandler errorHandler)
    {
        try
        {
            final Class<?> cls = Class.forName("uk.co.real_logic.artio.ilink.ILink3Offsets");
            return (AbstractILink3Offsets)cls.getConstructor().newInstance();
        }
        catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
            IllegalAccessException | InvocationTargetException e)
        {
            errorHandler.onError(e);
            return null;
        }
    }

    public abstract int seqNumOffset(int templateId);

    public abstract int seqNum(int templateId, DirectBuffer buffer, int messageOffset);

    public abstract int possRetransOffset(int templateId);

    public abstract int possRetrans(int templateId, DirectBuffer buffer, int messageOffset);

    public abstract int sendingTimeEpochOffset(int templateId);
}
