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
package uk.co.real_logic.fix_gateway.streams;

import uk.co.real_logic.fix_gateway.engine.framer.AdminCommand;
import uk.co.real_logic.fix_gateway.engine.framer.Framer;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryCommand;

public final class InactiveProcess implements AdminCommand, LibraryCommand
{
    private final int sessionId;

    public InactiveProcess(final int sessionId)
    {
        this.sessionId = sessionId;
    }

    public void execute(final Framer framer)
    {
        framer.onInactiveLibrary(sessionId);
    }

    public void execute(final FixLibrary library)
    {
        library.onInactiveGateway();
    }
}
