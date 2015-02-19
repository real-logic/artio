/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.session_management;

import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.session_management.SessionInformation.UNKNOWN;
import static uk.co.real_logic.fix_gateway.session_management.SessionState.CONNECTION_ESTABLISHED;

public final class InboundSession
{
    private final MilliClock clock;

    private SessionInformation info;

    public InboundSession(final long defaultInterval, final long connectionId, final MilliClock clock)
    {
        this.clock = clock;
        info = new SessionInformation(defaultInterval, clock.time() + defaultInterval,
                                      connectionId, UNKNOWN, CONNECTION_ESTABLISHED);
    }

    public void onLogin(final long heartbeatInterval, final long sequenceNumber)
    {

    }

    public void onHeartbeat()
    {

    }

    public void onLogoutRequest()
    {

    }

    public void onResend()
    {

    }

    public void testRequest()
    {

    }

}
