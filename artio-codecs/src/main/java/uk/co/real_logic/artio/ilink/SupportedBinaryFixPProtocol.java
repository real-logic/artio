/*
 * Copyright 2021 Monotonic Ltd.
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

import org.agrona.ErrorHandler;

public enum SupportedBinaryFixPProtocol
{
    ILINK_3("uk.co.real_logic.artio.ilink.Ilink3Protocol"),
    BINARY_ENTRYPOINT("uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointProtocol");

    private final String className;

    SupportedBinaryFixPProtocol(final String className)
    {
        this.className = className;
    }

    public BinaryFixPProtocol make(final ErrorHandler errorHandler)
    {
        try
        {
            final Class<?> cls = Class.forName(className);
            return (BinaryFixPProtocol)cls.newInstance();
        }
        catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e)
        {
            errorHandler.onError(e);
            return null;
        }
    }
}
