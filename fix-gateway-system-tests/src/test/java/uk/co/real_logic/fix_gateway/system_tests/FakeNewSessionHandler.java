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
package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.fix_gateway.admin.NewSessionHandler;
import uk.co.real_logic.fix_gateway.framer.session.Session;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;

public class FakeNewSessionHandler implements NewSessionHandler
{
    private GatewaySubscription subscription;
    private Session session;

    public void onConnect(final Session session, final GatewaySubscription subscription)
    {
        this.subscription = subscription;
    }

    public void onDisconnect(final Session session)
    {
        this.session = session;
    }

    public GatewaySubscription subscription()
    {
        return subscription;
    }

    public Session disconnectedSession()
    {
        return session;
    }
}
