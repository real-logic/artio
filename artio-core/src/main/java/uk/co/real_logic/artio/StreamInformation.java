/*
 * Copyright 2015-2024 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import uk.co.real_logic.artio.engine.EngineConfiguration;

public final class StreamInformation
{
    public static void print(
        final String name, final Subscription subscription, final CommonConfiguration configuration)
    {
        print(name, subscription, configuration.printAeronStreamIdentifiers());
    }

    public static void print(
        final String name, final Subscription subscription, final boolean printAeronStreamIdentifiers)
    {
        if (printAeronStreamIdentifiers)
        {
            System.out.printf(
                "%-40s - registrationId=%d,streamId=%d%n",
                name,
                subscription.registrationId(),
                subscription.streamId());
        }
    }

    public static void print(
        final String name, final ExclusivePublication publication, final EngineConfiguration configuration)
    {
        print(name, publication, configuration.printAeronStreamIdentifiers());
    }

    public static void print(
        final String name,
        final ExclusivePublication publication,
        final boolean printAeronStreamIdentifiers)
    {
        if (printAeronStreamIdentifiers)
        {
            System.out.printf(
                "%-40s - registrationId=%d,streamId=%d,sessionId=%d%n",
                name,
                publication.registrationId(),
                publication.streamId(),
                publication.sessionId());
        }
    }
}
