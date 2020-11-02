/*
 * Copyright 2020 Monotonic Limited.
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
package uk.co.real_logic.artio.ilink;

import iLinkBinary.MessageHeaderDecoder;
import iLinkBinary.MessageHeaderEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;

import java.util.function.Consumer;

import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.SOFH_LENGTH;

public class PrintingILinkMessageConsumer implements ILinkMessageConsumer
{
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final ILink3BusinessMessageDissector businessMessageLogger = new ILink3BusinessMessageDissector(this::log);
    private final StringBuilder builder = new StringBuilder();

    private final int inboundStreamId;

    public PrintingILinkMessageConsumer(final int inboundStreamId)
    {
        this.inboundStreamId = inboundStreamId;
    }

    private void log(final String prefix, final Consumer<StringBuilder> appendTo)
    {
        final StringBuilder builder = this.builder;
        builder.setLength(0);
        builder.append(prefix);
        appendTo.accept(builder);
        System.out.println(builder);
    }

    public void onBusinessMessage(
        final ILinkMessageDecoder iLinkMessage, final DirectBuffer buffer, final int start, final Header header)
    {
        int offset = start + SOFH_LENGTH;

        this.header.wrap(buffer, offset);
        final int templateId = this.header.templateId();
        final int blockLength = this.header.blockLength();
        final int version = this.header.version();
        final boolean inbound = header.streamId() == inboundStreamId;

        offset += MessageHeaderEncoder.ENCODED_LENGTH;

        businessMessageLogger.onBusinessMessage(templateId, buffer, offset, blockLength, version, inbound);
    }
}
