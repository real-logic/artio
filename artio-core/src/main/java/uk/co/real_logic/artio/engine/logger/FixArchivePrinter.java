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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner.MessageType;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.replication.StreamIdentifier;
import uk.co.real_logic.artio.sbe_util.MessageDumper;
import uk.co.real_logic.artio.sbe_util.MessageSchemaIr;
import uk.co.real_logic.sbe.json.JsonPrinter;

import java.io.PrintStream;
import java.time.ZonedDateTime;

import static java.lang.Long.parseLong;
import static uk.co.real_logic.artio.engine.logger.FixArchiveScanner.MessageType.SENT;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.*;

/**
 * Eg: -Dlogging.dir=/home/richard/monotonic/Fix-Engine/artio-system-tests/client-logs \
 * uk.co.real_logic.artio.engine.logger.ArchivePrinter
 */
public class FixArchivePrinter
{
    public static void main(final String[] args)
    {
        if (args.length < 2)
        {
            System.err.println("Usage: ArchivePrinter <channel> <streamId>");
            System.exit(-1);
        }

        String logFileDir = null;
        MessageType direction = SENT;
        FixMessageConsumer consumer = FixArchivePrinter::print;
        FixMessagePredicate predicate = FixMessagePredicates.alwaysTrue();

        String senderCompId = null;
        String targetCompId = null;

        for (final String arg : args)
        {
            final int eqIndex = arg.indexOf('=');
            final String optionName = arg.substring(2, eqIndex);
            final String optionValue = arg.substring(eqIndex);

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
                    senderCompId = optionValue;
                    break;

                case "target-comp-id":
                    targetCompId = optionValue;
                    break;

                case "direction":
                    direction = MessageType.valueOf(optionValue);
                    break;

                case "log-file-directory":
                    logFileDir = optionValue;
                    break;
            }
        }

        if (logFileDir == null)
        {
            System.err.println("Missing required --log-file-directory argument");
            System.exit(-1);
        }

        if (senderCompId != null && targetCompId != null)
        {
            predicate = sessionOf(senderCompId, targetCompId).and(predicate);
        }

        final FixArchiveScanner scanner = new FixArchiveScanner(logFileDir);
        scanner.scan("TODO", direction, filterBy(consumer, predicate), Throwable::printStackTrace);
    }

    private static void print(
        final FixMessageDecoder message,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        System.out.println(message.body());
    }
}
