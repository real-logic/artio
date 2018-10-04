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
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.protocol.StreamIdentifier;

import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.engine.logger.FixArchiveScanner.MessageType.SENT;

/**
 * Scan the archive for fix messages. Can be combined with predicates to create rich queries.
 *
 * @see FixMessageConsumer
 * @see FixMessagePredicate
 * @see FixMessagePredicates
 */
public class FixArchiveScanner
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final LogEntryHandler logEntryHandler = new LogEntryHandler();

    private final ArchiveScanner archiveScanner;

    private FixMessageConsumer handler;

    public enum MessageType
    {
        /** Messages sent from the engine to another FIX system */
        SENT,

        /** Messages received by the engine from another FIX system */
        RECEIVED,
    }

    public FixArchiveScanner(final String logFileDir)
    {
        archiveScanner = new ArchiveScanner(logFileDir);
    }

    public void scan(
        final String aeronChannel,
        final MessageType messageType,
        final FixMessageConsumer handler,
        final ErrorHandler errorHandler)
    {
        this.handler = handler;
        final StreamIdentifier id = new StreamIdentifier(
            aeronChannel, messageType == SENT ? OUTBOUND_LIBRARY_STREAM : INBOUND_LIBRARY_STREAM);
        archiveScanner.forEachFragment(id, logEntryHandler, errorHandler);
    }

    class LogEntryHandler implements FragmentHandler
    {
        @SuppressWarnings("FinalParameters")
        public void onFragment(
            final DirectBuffer buffer, int offset, final int length, final Header header)
        {
            messageHeader.wrap(buffer, offset);
            if (messageHeader.templateId() == FixMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                fixMessage.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

                handler.onMessage(fixMessage, buffer, offset, length, header);
            }
        }
    }

}
