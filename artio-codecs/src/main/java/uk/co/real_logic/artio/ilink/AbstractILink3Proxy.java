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

import io.aeron.ExclusivePublication;
import org.agrona.ErrorHandler;

import java.lang.reflect.InvocationTargetException;

public abstract class AbstractILink3Proxy
{
    public static AbstractILink3Proxy make(
        final ExclusivePublication publication, final ErrorHandler errorHandler)
    {
        try
        {
            final Class<?> cls = Class.forName("uk.co.real_logic.artio.ilink.ILink3Proxy");
            return (AbstractILink3Proxy)cls.getConstructor(long.class, ExclusivePublication.class)
                .newInstance(0, publication);
        }
        catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
            IllegalAccessException | InvocationTargetException e)
        {
            errorHandler.onError(e);
            return null;
        }
    }

    public abstract void connectionId(long connectionId);

    public abstract long sendSequence(
        long uuid, long nextSentSeqNo);
}
