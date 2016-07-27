/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.framer;

import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;

import java.util.List;

final class GatewaySessionsCommand implements AdminCommand
{
    private volatile List<SessionInfo> response;

    public void execute(final Framer framer)
    {
        framer.onGatewaySessions(this);
    }

    void success(final List<SessionInfo> response)
    {
        this.response = response;
    }

    List<SessionInfo> awaitResponse(final IdleStrategy idleStrategy)
    {
        List<SessionInfo> response;
        while ((response = this.response) == null)
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();
        return response;
    }
}
