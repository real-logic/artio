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
 * Callback that is invoked when a library is notified of new connections being made
 * to the gateway.
 *
 * @see LibraryConfiguration#newConnectHandler(NewConnectHandler)
 */
@FunctionalInterface
public interface NewConnectHandler
{
    /**
     * Called when a library knows of a new connection being made to the gateway.
     *
     * @param library the library object that this callback is associated with
     * @param connectionId the identifying number of the connection.
     * @param address the host/port combination of the inbound connection.
     */
    void onConnect(final FixLibrary library, final long connectionId, final String address);
}
