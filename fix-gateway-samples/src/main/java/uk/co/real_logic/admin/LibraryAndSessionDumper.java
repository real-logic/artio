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
package uk.co.real_logic.admin;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;

/**
 * An example of how to query the FIX Engine for library information
 */
public class LibraryAndSessionDumper implements Agent
{
    private final IdleStrategy idleStrategy = backoffIdleStrategy();

    private final FixEngine engine;

    public LibraryAndSessionDumper(final FixEngine engine)
    {
        this.engine = engine;
    }

    public int doWork() throws Exception
    {
        for (final LibraryInfo library : engine.libraries(idleStrategy))
        {
            System.out.println("--------------------------------");
            final String note = library.isAcceptor() ? " is Acceptor" : "";
            System.out.printf("Library %d %s\n", library.libraryId(), note);
            System.out.println("--------------------------------\n");
            System.out.println("| Id   | Remote Address ");
            for (final SessionInfo session : library.sessions())
            {
                System.out.printf("| %4d | %s", session.connectionId(), session.address());
            }
            System.out.println("--------------------------------\n");
        }

        return 1;
    }

    public String roleName()
    {
        return "Library and Session Dumper";
    }
}
