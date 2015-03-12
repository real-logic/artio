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
package uk.co.real_logic.fix_gateway.framer.session;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.commands.SessionManagerCommand;
import uk.co.real_logic.fix_gateway.commands.SessionManagerProxy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SessionManagerTest
{
    private Session mockSession = mock(Session.class);
    private OneToOneConcurrentArrayQueue<SessionManagerCommand> commandQueue = new OneToOneConcurrentArrayQueue<>(10);
    private SessionManagerProxy sessionManagerProxy = new SessionManagerProxy(commandQueue);
    private SessionManager sessionManager = new SessionManager(commandQueue);

    @Test
    public void shouldPollNewSessions() throws Exception
    {
        given:
        sessionManagerProxy.newSession(mockSession);

        when:
        sessionManager.doWork();

        then:
        verify(mockSession).poll();
    }
}
