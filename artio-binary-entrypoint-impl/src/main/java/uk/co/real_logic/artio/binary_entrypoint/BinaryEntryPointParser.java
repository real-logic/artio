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
package uk.co.real_logic.artio.binary_entrypoint;

import b3.entrypoint.fixp.sbe.EstablishDecoder;
import b3.entrypoint.fixp.sbe.MessageHeaderDecoder;
import b3.entrypoint.fixp.sbe.NegotiateDecoder;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.fixp.AbstractFixPParser;

import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

public class BinaryEntryPointParser extends AbstractFixPParser
{
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final NegotiateDecoder negotiate = new NegotiateDecoder();
    private final EstablishDecoder establish = new EstablishDecoder();

    public int templateId(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        return header.templateId();
    }

    public int blockLength(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        return header.blockLength();
    }

    public int version(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        return header.version();
    }

    public long onMessage(final DirectBuffer buffer, final int start)
    {
        int offset = start + SOFH_LENGTH;

        header.wrap(buffer, offset);
        final int templateId = header.templateId();
        final int blockLength = header.blockLength();
        final int version = header.version();

        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId)
        {
            case NegotiateDecoder.TEMPLATE_ID:
                negotiate.wrap(buffer, offset, blockLength, version);
                return 1;

            case EstablishDecoder.TEMPLATE_ID:
                establish.wrap(buffer, offset, blockLength, version);
                return 1;
        }

        return 1;
    }

    public BinaryEntryPointIdentification lookupIdentification(
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final int lastConnectPayload,
        final DirectBuffer messageBuffer,
        final int messageOffset,
        final int messageLength)
    {
        int offset = messageOffset + SOFH_LENGTH;

        header.wrap(messageBuffer, offset);
        final int templateId = header.templateId();
        final int blockLength = header.blockLength();
        final int version = header.version();

        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId)
        {
            case NegotiateDecoder.TEMPLATE_ID:
                negotiate.wrap(messageBuffer, offset, blockLength, version);
                return new BinaryEntryPointIdentification();

            case EstablishDecoder.TEMPLATE_ID:
                establish.wrap(messageBuffer, offset, blockLength, version);
                return new BinaryEntryPointIdentification();
        }

        throw new IllegalArgumentException("Template id: " + templateId + " isn't a negotiate or establish");
    }

    public long sessionId(final DirectBuffer buffer, final int start)
    {
        int offset = start + SOFH_LENGTH;

        header.wrap(buffer, offset);
        final int templateId = header.templateId();
        final int blockLength = header.blockLength();
        final int version = header.version();

        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId)
        {
            case NegotiateDecoder.TEMPLATE_ID:
                negotiate.wrap(buffer, offset, blockLength, version);
                return negotiate.sessionID();

            case EstablishDecoder.TEMPLATE_ID:
                establish.wrap(buffer, offset, blockLength, version);
                return establish.sessionID();
        }

        throw new IllegalArgumentException("Template id: " + templateId + " isn't a negotiate or establish");
    }
}
