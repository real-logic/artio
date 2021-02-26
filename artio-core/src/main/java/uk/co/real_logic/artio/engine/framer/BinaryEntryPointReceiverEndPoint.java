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
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.fixp.FixPProtocol;
import uk.co.real_logic.artio.fixp.FixPProtocolFactory;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.messages.InboundFixPConnectEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.BINARY_ENTRYPOINT_TYPE;

public class BinaryEntryPointReceiverEndPoint extends FixPReceiverEndPoint
{
    private static final int ACCEPTED_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH +
        InboundFixPConnectEncoder.BLOCK_LENGTH;

    private final ExclusivePublication publication;

    private long sessionId;
    private boolean requiresAuthentication = true;

    BinaryEntryPointReceiverEndPoint(
        final long connectionId,
        final TcpChannel channel,
        final int bufferSize,
        final ErrorHandler errorHandler,
        final Framer framer,
        final GatewayPublication publication,
        final int libraryId,
        final EpochNanoClock epochNanoClock,
        final long correlationId)
    {
        super(
            connectionId,
            channel,
            bufferSize,
            errorHandler,
            framer,
            publication,
            libraryId,
            epochNanoClock,
            correlationId,
            BINARY_ENTRYPOINT_TYPE);
        this.publication = publication.dataPublication();
    }

    void checkMessage(final MutableAsciiBuffer buffer, final int offset, final int messageSize)
    {
        if (requiresAuthentication)
        {
            final FixPProtocol protocol = FixPProtocolFactory.make(FixPProtocolType.BINARY_ENTRYPOINT, errorHandler);
            sessionId = protocol.makeParser(null).sessionId(buffer, offset);

            final MessageHeaderEncoder header = new MessageHeaderEncoder();
            final InboundFixPConnectEncoder inboundFixPConnect = new InboundFixPConnectEncoder();
            final UnsafeBuffer logonBuffer = new UnsafeBuffer(new byte[ACCEPTED_HEADER_LENGTH]);
            inboundFixPConnect
                .wrapAndApplyHeader(logonBuffer, 0, header)
                .connection(connectionId)
                .sessionId(sessionId)
                .protocolType(FixPProtocolType.BINARY_ENTRYPOINT)
                .lastReceivedSequenceNumber(0)
                .lastSentSequenceNumber(0)
                .lastConnectPayload(0)
                .messageLength(messageSize);

            final long position = publication.offer(
                logonBuffer, 0, ACCEPTED_HEADER_LENGTH,
                buffer, offset, messageSize);

            if (position < 0)
            {
                System.out.println("position = " + position); // TODO
            }

            requiresAuthentication = false;
        }
    }

    boolean requiresAuthentication()
    {
        return requiresAuthentication;
    }

    void trackDisconnect()
    {
    }
}
