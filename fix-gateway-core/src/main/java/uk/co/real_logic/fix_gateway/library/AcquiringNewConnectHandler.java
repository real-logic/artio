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

import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;

/**
 * {@link NewConnectHandler} implementation that tries to acquire any session
 * that has connected to the engine. Very useful for implementing simple 1-to-1
 * topology configurations between engine and library.
 */
public class AcquiringNewConnectHandler implements NewConnectHandler
{
    public void onConnect(final FixLibrary library, final long connectionId, final String address)
    {
        library.requestSession(connectionId, NO_MESSAGE_REPLAY);
    }
}
