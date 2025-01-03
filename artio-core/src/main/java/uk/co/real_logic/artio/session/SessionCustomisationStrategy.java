/*
 * Copyright 2015-2025 Real Logic Limited., Monotonic Ltd.
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
package uk.co.real_logic.artio.session;

import uk.co.real_logic.artio.builder.AbstractLogonEncoder;
import uk.co.real_logic.artio.builder.AbstractLogoutEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;

/**
 * Implement this interface if you want to alter logon or logoff messages with additional
 * logic in a way that's specific to your venue. Since the FIX Engine itself creates the
 * logon and logout messages this is the only way to modify them.
 *
 * @see uk.co.real_logic.artio.library.LibraryConfiguration
 */
public interface SessionCustomisationStrategy
{
    static SessionCustomisationStrategy none()
    {
        return new NoSessionCustomisationStrategy();
    }

    /**
     * Add additional fields or information to the logon message.
     *
     * @param logon the logon message about to be sent
     * @param sessionId the surrogate id for the Session that is being customised
     */
    void configureLogon(AbstractLogonEncoder logon, long sessionId);

    /**
     * Add additional fields or information to the logout message.
     *
     * @param logout the logout message about to be sent
     * @param sessionId the surrogate id for the Session that is being customised
     */
    void configureLogout(AbstractLogoutEncoder logout, long sessionId);

    /**
     * Add additional fields to the header of any message sent by Artio.
     *
     * @param header the header about to be sent
     * @param sessionId the surrogate id for the Session that is being customised
     */
    default void configureHeader(SessionHeaderEncoder header, long sessionId)
    {
    }
}
