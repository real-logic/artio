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
package uk.co.real_logic.fix_gateway.library;

import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.timing.LibraryTimers;

import static org.mockito.Mockito.mock;

public class LibraryPollerTest
{
    public static final int CONNECTION_ID = 1;
    public static final int SESSION_ID = 1;
    private LibraryTransport transport = mock(LibraryTransport.class);
    private FixCounters counters = mock(FixCounters.class);
    private FixLibrary library = mock(FixLibrary.class);

    private LibraryPoller libraryPoller = new LibraryPoller(
        new LibraryConfiguration(),
        new LibraryTimers(),
        counters,
        transport,
        library);

    @Ignore // TODO
    @Test
    public void shouldNotifyClientsOfSessionTimeouts()
    {
        final Reply<Session> reply = libraryPoller.initiate(SessionConfiguration.builder().build());

        /*libraryPoller.onManageConnection(
            libraryPoller.libraryId(),
            CONNECTION_ID,
            SESSION_ID,
            ConnectionType.INITIATOR,
            1,
            1,
            )*/
    }

}
