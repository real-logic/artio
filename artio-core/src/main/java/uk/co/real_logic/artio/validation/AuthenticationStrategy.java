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


import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.decoder.AbstractUserRequestDecoder;
import uk.co.real_logic.artio.messages.DisconnectReason;

/**
 * Implement this interface in order to add customisable checks to logon messages.
 *
 * You can implement the AuthenticationStrategy in two ways: an async authentication strategy
 * or a simple synchronous call.
 */
@FunctionalInterface
public interface AuthenticationStrategy
{
    static AuthenticationStrategy none()
    {
        return new NoAuthenticationStrategy();
    }

    static AuthenticationStrategy of(final MessageValidationStrategy delegate)
    {
        return (logon) -> delegate.validate(logon.header());
    }

    /**
     * Implement this method if your authentication strategy needs to engage in potentially long running
     * communications with external services, eg: talk over a network to an LDAP server.
     *
     * NB: if you're implementing this method then you shouldn't implement the {@link #authenticate(AbstractLogonDecoder)}
     * method.
     *
     * @param logon the logon message to authenticate. This is an object that gets allocated just to handle the logon
     *              message and is safe to use within your own code without copying.
     * @param authProxy the proxy to notify when you're ready to authenticate.
     */
    default void authenticateAsync(AbstractLogonDecoder logon, AuthenticationProxy authProxy)
    {
        if (authenticate(logon))
        {
            authProxy.accept();
        }
        else
        {
            authProxy.reject();
        }
    }

    /**
     * Implement this method if your authentication strategy call will be very quick. For example looking up a pair of
     * sender and target comp id in a local hashmap. This is a simpler approach than
     * {@link #authenticateAsync(AbstractLogonDecoder, AuthenticationProxy)} at the cost that it will block the Framer thread.
     *
     * NB: if you're implementing this method then you shouldn't implement the
     * {@link #authenticateAsync(AbstractLogonDecoder, AuthenticationProxy)} method.
     * @param logon the logon message to authenticate.
     * @return true to accept the new session, false to reject it.
     */
    boolean authenticate(AbstractLogonDecoder logon);

    /**
     * Hands a user request message to the authentication strategy. This is the only way to get access to the password
     * and newPassword fields of a user request message.
     *
     * User request messages may include a password change, and the passwords will be cleaned before they
     * arrive at the {@link uk.co.real_logic.artio.library.FixLibrary}. As a result we hand off UserRequest messages
     * to the AuthenticationStrategy before the password cleaning so that your authentication system can deal with
     * password changes. This message will still be sent to the approach {@link uk.co.real_logic.artio.session.Session}
     * object and processed as normal (sequence number updates, validation, etc.) just without the password fields.
     *
     * @param userRequest the user request message that has been received.
     * @param sessionId the session id of the session that received the message
     */
    default void onUserRequest(
        final AbstractUserRequestDecoder userRequest, final long sessionId)
    {
        // Deliberately blank for backwards compatibility
    }

    /**
     * Callback when a connection gets disconnected. This can be used to correlate with authentication events
     * or to cleanup resources associated with an authentication. In order to correlate authentication requests the
     * connection id parameter can be used. This will be the same passed value that is returned from
     * {@link AuthenticationProxy#connectionId()} when the authentication attempt originally happened.
     *
     * Note that at the time that the authentication strategy is invoked that the session id has not yet been assigned
     * to the session, only the connection id. So only the connection id can be used to correlate the authentication and
     * disconnect events. The session id is provided in order to correlation with other information or events relating
     * to the session generally.
     *
     * @param sessionId the session id of the session that has disconnected
     * @param connectionId the connection id of the session that has disconnected.
     * @param reason the reason of the session disconnect
     */
    default void onDisconnect(
        final long sessionId,
        final long connectionId,
        final DisconnectReason reason)
    {
        // Deliberately blank for backwards compatibility
    }
}
