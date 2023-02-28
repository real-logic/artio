/*
 * Copyright 2015-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.admin;

import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;

import java.util.List;

import static uk.co.real_logic.artio.CommonConfiguration.backoffIdleStrategy;

/**
 * An example of how to query the FIX Engine for library information
 */
public class LibraryAndSessionDumper
{
    private final IdleStrategy idleStrategy = backoffIdleStrategy();

    private final FixEngine engine;

    public LibraryAndSessionDumper(final FixEngine engine)
    {
        this.engine = engine;
    }

    public void printLibraries()
    {
        final Reply<List<LibraryInfo>> reply = engine.libraries();
        while (reply.isExecuting())
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();

        for (final LibraryInfo library : reply.resultIfPresent())
        {
            System.out.println("--------------------------------");
            System.out.printf("Library %d%n", library.libraryId());
            System.out.println("--------------------------------%n");
            System.out.println("| Id   | Remote Address %n");
            for (final ConnectedSessionInfo session : library.sessions())
            {
                System.out.printf("| %4d | %s", session.connectionId(), session.address());
            }
            System.out.println("--------------------------------%n");
        }
    }

}
