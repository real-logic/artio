/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.stress;

import org.agrona.concurrent.YieldingIdleStrategy;
import uk.co.real_logic.fix_gateway.SampleUtil;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

import java.io.IOException;

import static java.util.Collections.singletonList;

public final class StableLibrary
{
    public static void main(final String[] args) throws IOException
    {
        final LibraryConfiguration libraryConfiguration = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(SoleEngine.AERON_CHANNEL))
            .libraryIdleStrategy(new YieldingIdleStrategy());

        libraryConfiguration.replyTimeoutInMs(1000);

        try (FixLibrary library = SampleUtil.blockingConnect(libraryConfiguration))
        {
            System.out.println("Connected");

            while (library.isConnected())
            {
                library.poll(1);

                Thread.yield();
            }
        }
    }
}
