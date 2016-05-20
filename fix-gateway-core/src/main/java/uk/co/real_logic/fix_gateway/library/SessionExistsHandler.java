/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.library;

/**
 * Callback that is invoked when a library is notified of a session existing.
 *
 * This will either be called when a new session is accepted on the gateway or
 * when the library first connects.
 *
 * @see LibraryConfiguration#sessionExistsHandler(SessionExistsHandler)
 */
@FunctionalInterface
public interface SessionExistsHandler
{
    /**
     * @param library the library object that this callback is associated with
     * @param sessionId the identifying number of the session that now exists.
     * @param acceptorCompId
     * @param acceptorSubId
     * @param acceptorLocationId
     * @param initiatorCompId
     * @param username
     * @param password
     */
    void onSessionExists(
        final FixLibrary library,
        final long sessionId,
        final String acceptorCompId,
        final String acceptorSubId,
        final String acceptorLocationId,
        final String initiatorCompId,
        final String username,
        final String password);
}
