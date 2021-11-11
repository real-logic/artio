/*
 * Copyright 2019-2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.dictionary;

import org.agrona.LangUtil;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.*;

import java.lang.reflect.InvocationTargetException;

public interface FixDictionary
{
    String DEFAULT_FIX_DICTIONARY_NAME = "uk.co.real_logic.artio.FixDictionaryImpl";

    static FixDictionary of(final Class<? extends FixDictionary> fixDictionaryType)
    {
        try
        {
            return fixDictionaryType.getConstructor().newInstance();
        }
        catch (final NoSuchMethodException e)
        {
            LangUtil.rethrowUnchecked(e);
            throw new IllegalStateException();  // Never invoked
        }
        catch (final InstantiationException | IllegalAccessException | InvocationTargetException e)
        {
            LangUtil.rethrowUnchecked(e);
            throw new RuntimeException();  // Never invoked
        }
    }

    static Class<? extends FixDictionary> findDefault()
    {
        return find(DEFAULT_FIX_DICTIONARY_NAME);
    }

    @SuppressWarnings("unchecked")
    static Class<? extends FixDictionary> find(final String name)
    {
        try
        {
            return (Class<? extends FixDictionary>)Class.forName(name);
        }
        catch (final ClassNotFoundException e)
        {
            throw new IllegalStateException("No FIX Dictionary specified and default found on the classpath: '" +
                name + "'", e);
        }
    }

    String beginString();

    AbstractLogonEncoder makeLogonEncoder();

    AbstractResendRequestEncoder makeResendRequestEncoder();

    AbstractLogoutEncoder makeLogoutEncoder();

    AbstractHeartbeatEncoder makeHeartbeatEncoder();

    AbstractRejectEncoder makeRejectEncoder();

    AbstractTestRequestEncoder makeTestRequestEncoder();

    AbstractSequenceResetEncoder makeSequenceResetEncoder();

    AbstractBusinessMessageRejectEncoder makeBusinessMessageRejectEncoder();

    AbstractLogonDecoder makeLogonDecoder();

    AbstractLogoutDecoder makeLogoutDecoder();

    AbstractRejectDecoder makeRejectDecoder();

    AbstractTestRequestDecoder makeTestRequestDecoder();

    AbstractSequenceResetDecoder makeSequenceResetDecoder();

    AbstractHeartbeatDecoder makeHeartbeatDecoder();

    AbstractResendRequestDecoder makeResendRequestDecoder();

    AbstractUserRequestDecoder makeUserRequestDecoder();

    SessionHeaderDecoder makeHeaderDecoder();

    SessionHeaderEncoder makeHeaderEncoder();
}
