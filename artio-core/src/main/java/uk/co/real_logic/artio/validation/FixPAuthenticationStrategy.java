/*
 * Copyright 2015-2024 Real Logic Limited.
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


import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.fixp.FixPContext;

/**
 * Authentication Strategy for a FIXP Session. Implement this interface in order to add customisable checks to for
 * negotiate and establish messages. Unlike FIX Artio doesn't support a synchronous authentication method.
 *
 * When Artio receives a "Negotiate" message or an "Establish" message in an attempt to re-establish the connection then
 * the {@link #authenticate(FixPContext, FixPAuthenticationProxy)} method is invoked.
 *
 * Your implementation can be configured using the
 * {@link EngineConfiguration#fixPAuthenticationStrategy(FixPAuthenticationStrategy)} method.
 */
@FunctionalInterface
public interface FixPAuthenticationStrategy
{
    /**
     * Returns an authentication strategy that accepts any logon attempt. This is the default authentication strategy
     * used by Artio.
     *
     * @return the authentication strategy.
     */
    static FixPAuthenticationStrategy none()
    {
        return (sessionId, authProxy) -> authProxy.accept();
    }

    /**
     * This method is invoked when an attempt to authentication a connection is made with either the negotiate or
     * establish messages. In order to uniquely identify the type of connection that is being attempted you are
     * passed the context object for the session in question. This contains identifying fields that are sent on either
     * Negotiate or Establish messages. The fields are defined on a FIXP implementation by implementation basis.
     *
     * In order to accept or reject the connection invoke the appropriate methods on the
     * {@link FixPAuthenticationProxy}.
     *
     * @param context the session identification for the FIXP session, this can be cast-ed to the session
     *                  identification type for your specific FIXP session, for example
     *                  <code>BinaryEntryPointIdentification</code> for the Binary Entrypoint protocol.
     * @param authProxy the proxy to notify when you're ready to authenticate.
     */
    void authenticate(FixPContext context, FixPAuthenticationProxy authProxy);
}
