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
package uk.co.real_logic.fix_gateway.session;

import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class SessionIdsTest
{
    private Queue<? super NewSessionId> stubQueue = new ConcurrentLinkedDeque<>();
    private SessionIds sessionIds = new SessionIds(stubQueue);

    @Test
    public void newSessionIdsNotifyQueue()
    {
        sessionIds.onLogon("a");

        sessionIdEnqueuedOnce();
    }

    @Test
    public void existingSessionIdsDoNotNotifyQueue()
    {
        sessionIds.onLogon("a");
        sessionIds.onLogon("a");

        sessionIdEnqueuedOnce();
    }

    @Test
    public void sessionIdsAreUnique()
    {
        assertNotEquals(sessionIds.onLogon("a"), sessionIds.onLogon("b"));
    }

    private void sessionIdEnqueuedOnce()
    {
        assertThat(stubQueue, hasSize(1));
    }
}
