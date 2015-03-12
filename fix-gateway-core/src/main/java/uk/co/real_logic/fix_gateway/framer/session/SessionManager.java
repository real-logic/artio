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

import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.commands.SessionManagerCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class SessionManager implements Agent
{

    private final Consumer<SessionManagerCommand> onCommandFunc = this::onCommand;
    private final List<Session> sessions = new ArrayList<>();

    private final OneToOneConcurrentArrayQueue<SessionManagerCommand> commandQueue;

    public SessionManager(final OneToOneConcurrentArrayQueue<SessionManagerCommand> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public int doWork() throws Exception
    {
        return commandQueue.drain(onCommandFunc) + pollSessions();
    }

    private int pollSessions()
    {
        for (final Session session: sessions)
        {
            session.poll();
        }
        return sessions.size();
    }

    private void onCommand(final SessionManagerCommand command)
    {
        command.execute(this);
    }

    public void onClose()
    {
        sessions.forEach(Session::disconnect);
    }

    public void onNewSession(final Session session)
    {
        sessions.add(session);
    }

    public String roleName()
    {
        return "Session Manager";
    }

}
