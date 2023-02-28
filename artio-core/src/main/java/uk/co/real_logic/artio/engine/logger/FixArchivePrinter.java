/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.messages.MessageStatus;

import java.util.function.Predicate;

import static java.lang.Long.parseLong;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_ARCHIVE_SCANNER_STREAM;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.*;

/**
 * Eg:
 * java uk.co.real_logic.artio.engine.logger.FixArchivePrinter \
 *   --log-file-dir=artio-system-tests/acceptor-logs/ \
 *   --aeron-channel=aeron:ipc
 *
 * NB: this tool can also be used with iLink3 if the binary has been built with iLink3 support.
 */
public final class FixArchivePrinter
{
    public static void main(final String[] args)
    {
        new FixArchivePrinter().scan(args);
    }

    private FixPProtocolType fixPProtocolType = FixPProtocolType.ILINK_3;
    private String logFileDir = null;
    private final IntHashSet queryStreamIds = new IntHashSet();
    private String aeronDirectoryName = null;
    private String aeronChannel = null;
    private String offlineArchiveDirectoryName = null;
    private int archiveScannerStreamId = DEFAULT_ARCHIVE_SCANNER_STREAM;
    private FixMessagePredicate predicate = FixMessagePredicates.alwaysTrue();
    private boolean follow = false;
    private boolean fixp = false;
    private Class<? extends FixDictionary> fixDictionaryType = null;
    private Predicate<SessionHeaderDecoder> headerPredicate = null;

    private void scan(final String[] args)
    {
        parseArgs(args);
        validateArgs();

        final ArchivingMediaDriver archivingMediaDriver = startArchiverIfNeeded();
        try
        {
            scanArchive(aeronDirectoryName, aeronChannel, queryStreamIds, predicate, follow, headerPredicate,
                archiveScannerStreamId, fixDictionaryType, fixPProtocolType, logFileDir);
        }
        finally
        {
            CloseHelper.close(archivingMediaDriver);
        }
    }

    private ArchivingMediaDriver startArchiverIfNeeded()
    {
        if (offlineArchiveDirectoryName == null)
        {
            return null;
        }

        return ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName),
            new Archive.Context().archiveDirectoryName(offlineArchiveDirectoryName));
    }

    private void parseArgs(final String[] args)
    {
        for (final String arg : args)
        {
            final int eqIndex = arg.indexOf('=');
            final String optionName = eqIndex != -1 ? arg.substring(2, eqIndex) : arg.substring(2);

            // Options without arguments
            switch (optionName)
            {
                case "help":
                    printHelp();
                    System.exit(0);
                    break; // checkstyle

                case "follow":
                    follow = true;
                    break;

                case "fixp":
                case "ilink":
                    fixp = true;
                    break;

                default:
                    requiredArgument(eqIndex);
            }

            final String optionValue = arg.substring(eqIndex + 1);

            switch (optionName)
            {
                case "from":
                    predicate = from(parseLong(optionValue)).and(predicate);
                    break;
                case "to":
                    predicate = to(parseLong(optionValue)).and(predicate);
                    break;
                case "message-types":
                    final String[] messageTypes = optionValue.split(",");
                    predicate = messageTypeOf(messageTypes).and(predicate);
                    break;
                case "sender-comp-id":
                    headerPredicate = safeAnd(headerPredicate, senderCompIdOf(optionValue));
                    break;
                case "target-comp-id":
                    headerPredicate = safeAnd(headerPredicate, targetCompIdOf(optionValue));
                    break;
                case "sender-sub-id":
                    headerPredicate = safeAnd(headerPredicate, senderSubIdOf(optionValue));
                    break;
                case "target-sub-id":
                    headerPredicate = safeAnd(headerPredicate, targetSubIdOf(optionValue));
                    break;
                case "sender-location-id":
                    headerPredicate = safeAnd(headerPredicate, senderLocationIdOf(optionValue));
                    break;
                case "target-location-id":
                    headerPredicate = safeAnd(headerPredicate, targetLocationIdOf(optionValue));
                    break;
                case "query-stream-id":
                    queryStreamIds.add(Integer.parseInt(optionValue));
                    break;
                case "archive-scanner-stream-id":
                    archiveScannerStreamId = Integer.parseInt(optionValue);
                    break;
                case "aeron-dir-name":
                    aeronDirectoryName = optionValue;
                    break;
                case "aeron-channel":
                    aeronChannel = optionValue;
                    break;
                case "offline-archive-dir":
                    offlineArchiveDirectoryName = optionValue;
                    break;
                case "fix-dictionary":
                    fixDictionaryType = FixDictionary.find(optionValue);
                    break;
                case "fixp-protocol":
                    fixPProtocolType = FixPProtocolType.valueOf(optionValue.toUpperCase());
                    break;
                case "log-file-dir":
                    logFileDir = optionValue;
                    break;
            }
        }
    }

    private void validateArgs()
    {
        if (fixDictionaryType == null && !fixp)
        {
            fixDictionaryType = FixDictionary.findDefault();
        }

        if (queryStreamIds.isEmpty())
        {
            queryStreamIds.add(DEFAULT_OUTBOUND_LIBRARY_STREAM);
        }

        requiredArgument(aeronDirectoryName, "aeron-dir-name");
        requiredArgument(aeronChannel, "aeron-channel");
    }

    private static void requiredArgument(final int eqIndex)
    {
        if (eqIndex == -1)
        {
            System.err.println("--ilink, --help and --follow are the only options that don't take a value");
            printHelp();
            System.exit(-1);
        }
    }

    private static void scanArchive(
        final String aeronDirectoryName,
        final String aeronChannel,
        final IntHashSet queryStreamIds,
        final FixMessagePredicate otherPredicate,
        final boolean follow,
        final Predicate<SessionHeaderDecoder> headerPredicate,
        final int archiveScannerStreamId,
        final Class<? extends FixDictionary> fixDictionaryType,
        final FixPProtocolType fixPProtocolType,
        final String logFileDir)
    {
        final FixDictionary fixDictionary = fixDictionaryType == null ? null : FixDictionary.of(fixDictionaryType);
        FixMessagePredicate predicate = otherPredicate;
        if (headerPredicate != null)
        {
            predicate = whereHeader(fixDictionary, headerPredicate).and(predicate);
        }

        final FixArchiveScanner.Configuration configuration = new FixArchiveScanner.Configuration()
            .aeronDirectoryName(aeronDirectoryName)
            .idleStrategy(CommonConfiguration.backoffIdleStrategy());

        if (logFileDir != null)
        {
            configuration.logFileDir(logFileDir);
        }

        try (FixArchiveScanner scanner = new FixArchiveScanner(configuration))
        {
            System.out.println("Starting Scan ... ");
            scanner.scan(
                aeronChannel,
                queryStreamIds,
                filterBy(FixArchivePrinter::print, predicate),
                new LazyFixPMessagePrinter(DEFAULT_INBOUND_LIBRARY_STREAM, fixPProtocolType),
                follow,
                archiveScannerStreamId);
        }
    }

    private static void requiredArgument(final String argument, final String description)
    {
        if (argument == null)
        {
            System.err.printf("Missing required --%s argument%n", description);
            printHelp();
            System.exit(-1);
        }
    }

    private static void printHelp()
    {
        System.out.println("FixArchivePrinter Options");
        System.out.println("All options are specified in the form: --optionName=optionValue");

        printOption(
            "aeron-dir-name",
            "Specifies the directory to use for archiving, should be the same as your " +
            "aeronContext.aeronDirectoryName()",
            true);
        printOption(
            "aeron-channel",
            "Specifies the aeron channel that was used to by the engine",
            true);

        printOption(
            "offline-archive-dir",
            "Enable offline mode using the given aeron archive directory. This is a good way to inspect the" +
            " directory of a shutdown Artio instance. It starts a media driver and proceeds to inspect the provided " +
            "aeron archive directory",
            false);
        printOption(
            "fix-dictionary",
            "The class name of the Fix Dictionary to use, default is used if this is not provided",
            false);
        printOption(
            "ilink",
            "Deprecated: use --fixp.",
            false);
        printOption(
            "fixp",
            "Suppresses the need to provide a fix dictionary on the classpath - used for situations where" +
            " only FIXP messages will be printed out",
            false);
        printOption(
            "fixp-protocol",
            "Specifies the FIXP protocol type to be used to interpret protocol messages, defaults to iLink3",
            false);
        printOption(
            "from",
            "Time in precision of CommonConfiguration.clock() that messages are not earlier than",
            false);
        printOption(
            "to",
            "Time in precision of CommonConfiguration.clock() that messages are not later than",
            false);
        printOption(
            "message-types",
            "Comma separated list of the message types (35=) that are printed",
            false);
        printOption(
            "sender-comp-id",
            "Only print messages where the header's sender comp id field matches this",
            false);
        printOption(
            "target-comp-id",
            "Only print messages where the header's sender comp id field matches this",
            false);
        printOption(
            "sender-sub-id",
            "Only print messages where the header's sender comp id field matches this",
            false);
        printOption(
            "target-sub-id",
            "Only print messages where the header's sender comp id field matches this",
            false);
        printOption(
            "sender-location-id",
            "Only print messages where the header's sender comp id field matches this",
            false);
        printOption(
            "target-location-id",
            "Only print messages where the header's sender comp id field matches this",
            false);
        printOption(
            "query-stream-id",
            "Only print messages where the query-stream-id matches this." +
            " This should be your configuration.inboundLibraryStream() or configuration.outboundLibraryStream().  " +
            "Defaults to outbound. Can be used twice in order to print both inbound and outbound streams.",
            false);
        printOption(
            "follow",
            "Continue to print out archive messages for a recording that is still in flight. defaults to off",
            false);
        printOption(
            "help",
            "Only prints this help message.",
            false);
        printOption(
            "log-file-dir",
            "Specifies a logFileDir option, this should be the same as provided to your EngineConfiguration." +
            "  This can be used to optimize scans that are time based",
            false);
    }

    private static void printOption(final String name, final String description, final boolean required)
    {
        System.out.printf("  --%-20s [%s] - %s%n", name, required ? "required" : "optional", description);
    }

    private static <T> Predicate<T> safeAnd(final Predicate<T> left, final Predicate<T> right)
    {
        return left == null ? right : left.and(right);
    }

    private static void print(
        final FixMessageDecoder message,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ArtioLogHeader header)
    {
        final MessageStatus status = message.status();
        final long timestamp = message.timestamp();
        final String body = message.body();
        System.out.printf("%1$20s: %2$s (%3$s)%n", timestamp, body, status);
    }

}
