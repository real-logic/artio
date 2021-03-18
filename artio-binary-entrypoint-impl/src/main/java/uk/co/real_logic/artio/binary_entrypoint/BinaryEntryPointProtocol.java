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

import io.aeron.ExclusivePublication;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.fixp.AbstractFixPStorage;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.fixp.FixPContext;
import uk.co.real_logic.artio.fixp.FixPProtocol;
import uk.co.real_logic.artio.library.FixPSessionOwner;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.BINARY_ENTRYPOINT_TYPE;

public class BinaryEntryPointProtocol extends FixPProtocol
{
    public BinaryEntryPointProtocol()
    {
        super(FixPProtocolType.BINARY_ENTRYPOINT, BINARY_ENTRYPOINT_TYPE);
    }

    public BinaryEntryPointParser makeParser(final FixPConnection connection)
    {
        return new BinaryEntryPointParser((InternalBinaryEntrypointConnection)connection);
    }

    public BinaryEntryPointProxy makeProxy(
        final ExclusivePublication publication, final EpochNanoClock epochNanoClock)
    {
        return new BinaryEntryPointProxy(0, publication);
    }

    public BinaryEntryPointOffsets makeOffsets()
    {
        return new BinaryEntryPointOffsets();
    }

    public InternalFixPConnection makeAcceptorConnection(
        final long connectionId,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final FixPSessionOwner owner,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final long lastConnectPayload,
        final FixPContext context,
        final CommonConfiguration configuration)
    {
        return new InternalBinaryEntrypointConnection(
            connectionId,
            outboundPublication,
            inboundPublication,
            libraryId,
            owner,
            lastReceivedSequenceNumber,
            lastSentSequenceNumber,
            lastConnectPayload,
            configuration,
            (BinaryEntryPointContext)context);
    }

    public AbstractFixPStorage makeCodecs(final EpochNanoClock clock)
    {
        return new BinaryEntryPointStorage();
    }
}
