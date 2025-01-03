/*
 * Copyright 2015-2025 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.validation;

import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;

/**
 * Interface to notify the gateway whether a FIX Logon should be authenticated or not.
 *
 * Either only call the <code>accept </code>or the <code>reject()</code> method. See
 * {@link AbstractAuthenticationProxy} for more options to invoke.
 */
public interface AuthenticationProxy extends AbstractAuthenticationProxy
{
    /**
     * Call this method to accept the authentication and specify a FIX Dictionary based upon the logon message.
     *
     * @param fixDictionaryClass the FIX dictionary that you wish to use for this class.
     * @throws IllegalStateException if <code>accept()</code> or <code>reject()</code> has already been
     * successfully called.
     */
    void accept(Class<? extends FixDictionary> fixDictionaryClass);

    /**
     * Call this method to reject the authentication with a custom message.
     *
     * @param encoder the encoder that defines the message. This encoder should not be re-used for other rejects.
     * @param lingerTimeoutInMs the time to wait after encoding this message before closing the TCP connection.
     *
     * @throws NullPointerException if encoder is null
     * @throws IllegalArgumentException if lingerTimeoutInMs is negative
     * @throws IllegalStateException if <code>accept()</code> or <code>reject()</code> has already been
     * successfully called.
     */
    void reject(Encoder encoder, long lingerTimeoutInMs);
}
