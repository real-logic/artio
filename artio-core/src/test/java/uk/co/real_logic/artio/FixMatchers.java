/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio;

import org.hamcrest.Matcher;
import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.session.Session;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static uk.co.real_logic.artio.util.CustomMatchers.hasFluentProperty;
import static uk.co.real_logic.artio.util.CustomMatchers.hasResult;

public final class FixMatchers
{
    private FixMatchers()
    {
    }

    public static Matcher<ConnectedSessionInfo> hasConnectionId(final long connectionId)
    {
        return hasResult("connectionId", ConnectedSessionInfo::connectionId, equalTo(connectionId));
    }

    public static Matcher<ConnectedSessionInfo> hasSessionId(final long sessionId)
    {
        return hasResult("sessionId", ConnectedSessionInfo::sessionId, equalTo(sessionId));
    }

    public static Matcher<FixLibrary> isConnected()
    {
        return hasResult("isConnected", FixLibrary::isConnected, equalTo(true));
    }

    public static Matcher<Session> hasSequenceIndex(final int sequenceIndex)
    {
        return hasResult("sequenceIndex", Session::sequenceIndex, equalTo(sequenceIndex));
    }

    public static Matcher<LibraryInfo> matchesLibrary(final int libraryId)
    {
        return hasFluentProperty("libraryId", is(libraryId));
    }
}
