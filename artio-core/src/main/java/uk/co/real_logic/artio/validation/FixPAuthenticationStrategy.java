/*
 * Copyright 2015-2021 Real Logic Limited.
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


import uk.co.real_logic.artio.fixp.FixPIdentification;

/**
 * Authentication Strategy for a FIXP Session
 */
@FunctionalInterface
public interface FixPAuthenticationStrategy
{
    static FixPAuthenticationStrategy none()
    {
        return (sessionId, authProxy) -> authProxy.accept();
    }

    /**
     * Asynchronous authentication strategy.
     *
     * @param sessionId the session identification for the FIXP session, this can be casted to the the session
     *                  identification type for your specific FIXP session, for example
     *                  <code>BinaryEntryPointIdentification</code> for the Binary Entrypoint protocol.
     * @param authProxy the proxy to notify when you're ready to authenticate.
     */
    void authenticate(FixPIdentification sessionId, FixPAuthenticationProxy authProxy);
}
