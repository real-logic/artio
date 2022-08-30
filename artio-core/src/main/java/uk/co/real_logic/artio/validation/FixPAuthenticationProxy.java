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
package uk.co.real_logic.artio.validation;

import uk.co.real_logic.artio.fixp.FixPContext;

/**
 * Interface to notify the gateway whether a FIXP acceptor session should be authenticated or not.
 *
 * Either only call the <code>accept </code>or the <code>reject()</code> method. See
 *  * {@link AbstractAuthenticationProxy} for more options to invoke.
 */
public interface FixPAuthenticationProxy extends AbstractAuthenticationProxy
{
    /**
     * Call this method to reject the authentication. This overload of
     * {@link AbstractAuthenticationProxy#reject()} allows you to provide a reject code. Reject codes
     * are specific to the FixP implementation, and often Negotiate and Establish messages have different
     * reject codes. The {@link FixPContext#fromNegotiate()} method can be used in order to identify
     * whether a <code>Negotiate</code> or an <code>Establish</code> message is being used.
     *
     * For example if you're using BinaryEntryPoint, then it's <code>EstablishRejectCode</code> enum
     * can be passed as an argument if you've received an <code>Establish</code> message and the
     * <code>NegotiationRejectCode</code> enum can be used in response to <code>Negotiate</code>.
     *
     * @throws IllegalArgumentException if the wrong type of reject code is provided.
     * @throws IllegalStateException if <code>accept()</code> or <code>reject()</code> has already been
     * successfully called.
     * @param rejectCode the enum value of the reject code to use.
     */
    void reject(Enum<?> rejectCode);
}
