/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import io.aeron.CommonContext;
import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner;
import uk.co.real_logic.artio.engine.logger.FixMessageConsumer;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicate;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicates;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_ARCHIVE_SCANNER_STREAM;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ArchiveScannerBenchmark
{
    public static void main(final String[] args)
    {
        System.out.println("Running in: " + new File(".").getAbsolutePath());

        final long start = Long.parseLong(args[0]);
        final long end = Long.parseLong(args[1]);
        final boolean enableIndexScan = Boolean.parseBoolean(args[2]);
        final boolean includePredicate = Boolean.parseBoolean(args[3]);
        final int totalRuns = Integer.parseInt(args[4]);

        final FixArchiveScanner.Configuration context = new FixArchiveScanner.Configuration()
            .aeronDirectoryName(CommonContext.getAeronDirectoryName())
            .idleStrategy(CommonConfiguration.backoffIdleStrategy())
            .logFileDir(ACCEPTOR_LOGS)
            .enableIndexScan(enableIndexScan);

        try (ArchivingMediaDriver mediaDriver = TestFixtures.launchMediaDriverWithDirs();
            FixArchiveScanner scanner = new FixArchiveScanner(context))
        {
            for (int i = 0; i < totalRuns; i++)
            {
                final IntHashSet queryStreamIds = new IntHashSet();
                queryStreamIds.add(CommonConfiguration.DEFAULT_OUTBOUND_LIBRARY_STREAM);

                final List<String> messages = new ArrayList<>();
                final FixMessageConsumer fixMessageConsumer =
                    (message, buffer, offset, length, header) -> messages.add(message.body());

                final FixDictionary fixDictionary = FixDictionary.of(FixDictionary.findDefault());
                final FixMessagePredicate timeFilter = FixMessagePredicates.between(start, end + 1);
                final Predicate<SessionHeaderDecoder> sessionFilter = targetCompIdOf(INITIATOR_ID)
                    .or(senderCompIdOf(ACCEPTOR_ID));
                final FixMessagePredicate predicate = whereHeader(fixDictionary, sessionFilter)
                    .and(timeFilter);

                final long scanStart = System.nanoTime();
                final FixMessageConsumer consumer = includePredicate ?
                    filterBy(fixMessageConsumer, predicate) : fixMessageConsumer;
                scanner.scan(
                    IPC_CHANNEL,
                    queryStreamIds,
                    consumer,
                    null,
                    false,
                    DEFAULT_ARCHIVE_SCANNER_STREAM);

                final long scanEnd = System.nanoTime();
                System.out.println("message scan time = " + TimeUnit.NANOSECONDS.toMillis(scanEnd - scanStart));
                System.out.println("messages = " + messages.size());
            }
        }
    }
}
