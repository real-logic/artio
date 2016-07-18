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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.util.function.Consumer;

public class FixArchiveScanner
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final LogEntryHandler logEntryHandler = new LogEntryHandler();

    private final ArchiveScanner archiveScanner;

    private Consumer<FixMessageDecoder> handler;

    public FixArchiveScanner(final String logFileDir)
    {
        archiveScanner = new ArchiveScanner(logFileDir);
    }

    public void forEachMessage(
        final StreamIdentifier id,
        final Consumer<FixMessageDecoder> handler,
        final ErrorHandler errorHandler)
    {
        this.handler = handler;
        archiveScanner.forEachFragment(id, logEntryHandler, errorHandler);
    }

    private class LogEntryHandler implements FragmentHandler
    {
        public void onFragment(
            final DirectBuffer buffer, int offset, final int length, final Header header)
        {
            messageHeader.wrap(buffer, offset);
            if (messageHeader.templateId() == FixMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                fixMessage.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

                handler.accept(fixMessage);
            }
        }
    }

}
