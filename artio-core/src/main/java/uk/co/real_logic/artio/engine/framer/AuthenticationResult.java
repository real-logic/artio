/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.messages.DisconnectReason;

final class AuthenticationResult
{
    private static final long NO_REQUIRED_POSITION = -1;

    static final AuthenticationResult DUPLICATE_SESSION =
        new AuthenticationResult(DisconnectReason.DUPLICATE_SESSION);
    static final AuthenticationResult FAILED_AUTHENTICATION =
        new AuthenticationResult(DisconnectReason.FAILED_AUTHENTICATION);
    static final AuthenticationResult INVALID_CONFIGURATION_NOT_LOGGING_MESSAGES =
        new AuthenticationResult(DisconnectReason.INVALID_CONFIGURATION_NOT_LOGGING_MESSAGES);

    private final GatewaySession session;
    private final DisconnectReason reason;
    private final long requiredPosition;

    private AuthenticationResult(final DisconnectReason reason)
    {
        this.reason = reason;
        this.session = null;
        this.requiredPosition = NO_REQUIRED_POSITION;
    }

    AuthenticationResult(final GatewaySession session)
    {
        this.session = session;
        this.reason = null;
        this.requiredPosition = NO_REQUIRED_POSITION;
    }

    AuthenticationResult(final GatewaySession session, final long requiredPosition)
    {
        this.session = session;
        this.reason = null;
        this.requiredPosition = requiredPosition;
    }

    boolean isValid()
    {
        return session != null;
    }

    boolean isBackPressured()
    {
        return requiredPosition != NO_REQUIRED_POSITION;
    }

    public long requiredPosition()
    {
        return requiredPosition;
    }

    DisconnectReason reason()
    {
        return reason;
    }
}
