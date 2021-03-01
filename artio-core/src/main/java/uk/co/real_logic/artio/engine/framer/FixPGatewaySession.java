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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.ExclusivePublication;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.fixp.AbstractFixPParser;
import uk.co.real_logic.artio.messages.ConnectionType;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.messages.InboundFixPConnectEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

public class FixPGatewaySession extends GatewaySession
{
    private static final int ACCEPTED_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH +
        InboundFixPConnectEncoder.BLOCK_LENGTH;

    private final FixPProtocolType protocolType;
    private final AbstractFixPParser parser;
    private final ExclusivePublication publication;

    private byte[] firstMessage;

    FixPGatewaySession(
        final long connectionId,
        final long sessionId,
        final String address,
        final ConnectionType connectionType,
        final long authenticationTimeoutInMs,
        final FixPProtocolType protocolType,
        final AbstractFixPParser parser,
        final ExclusivePublication publication)
    {
        super(connectionId, sessionId, address, connectionType, authenticationTimeoutInMs);
        this.protocolType = protocolType;
        this.parser = parser;
        this.publication = publication;
    }

    public String address()
    {
        return address;
    }

    int poll(final long timeInMs, final long timeInNs)
    {
        return 0;
    }

    long lastLogonTime()
    {
        return 0;
    }

    void acceptorSequenceNumbers(final int lastSentSequenceNumber, final int lastReceivedSequenceNumber)
    {
    }

    // Called with the first message sent in the FIXP stream
    public void onLogon(final MutableAsciiBuffer buffer, final int offset, final int messageSize)
    {
        firstMessage = new byte[messageSize];
        buffer.getBytes(offset, firstMessage, 0, messageSize);

        // TODO: add in pending acceptor logon lifecycle

        sessionId = parser.sessionId(buffer, offset);

        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final InboundFixPConnectEncoder inboundFixPConnect = new InboundFixPConnectEncoder();
        final UnsafeBuffer logonBuffer = new UnsafeBuffer(new byte[ACCEPTED_HEADER_LENGTH]);
        inboundFixPConnect
            .wrapAndApplyHeader(logonBuffer, 0, header)
            .connection(connectionId)
            .sessionId(sessionId)
            .protocolType(protocolType)
            .messageLength(messageSize);

        final long position = publication.offer(
            logonBuffer, 0, ACCEPTED_HEADER_LENGTH,
            buffer, offset, messageSize);

        if (position < 0)
        {
            System.out.println("position = " + position); // TODO
        }
    }

    public FixPProtocolType protocolType()
    {
        return protocolType;
    }

    public byte[] firstMessage()
    {
        return firstMessage;
    }
}
