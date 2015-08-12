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

import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.LibraryInfo;

import java.util.List;
import java.util.concurrent.locks.LockSupport;

public final class QueryLibraries implements AdminCommand
{
    private volatile List<LibraryInfo> response;

    public void execute(final Framer framer)
    {
        framer.onQueryLibraries(this);
    }

    public void respond(final List<LibraryInfo> response)
    {
        this.response = response;
    }

    public List<LibraryInfo> awaitResponse()
    {
        List<LibraryInfo> response;
        while ((response = this.response) == null)
        {
            LockSupport.parkNanos(FixEngine.COMMAND_QUEUE_IDLE);
        }
        return response;
    }
}
