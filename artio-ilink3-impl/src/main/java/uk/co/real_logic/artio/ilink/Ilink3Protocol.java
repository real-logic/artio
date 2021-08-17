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
package uk.co.real_logic.artio.ilink;

import iLinkBinary.NegotiationResponse501Decoder;
import iLinkBinary.Negotiate500Encoder;
import io.aeron.ExclusivePublication;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.engine.logger.FixPSequenceNumberHandler;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.fixp.*;
import uk.co.real_logic.artio.library.FixPSessionOwner;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.sbe.ir.Ir;

import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.CME_ENCODING_TYPE;

public class Ilink3Protocol extends FixPProtocol
{
    private static final String SBE_IR_FILE = "ilinkbinary.sbeir";
    private static final class LazyLoader
    {
        static final Ir IR = AbstractFixPOffsets.loadSbeIr(Negotiate500Encoder.class, SBE_IR_FILE);
    }

    public static Ir loadSbeIr()
    {
        return LazyLoader.IR;
    }

    public static <T> T unsupported()
    {
        throw new UnsupportedOperationException("iLink3 is only implemented as an initiator");
    }

    public Ilink3Protocol()
    {
        super(FixPProtocolType.ILINK_3, CME_ENCODING_TYPE, NegotiationResponse501Decoder.TEMPLATE_ID,
            "iLinkBinary");
    }

    public ILink3Parser makeParser(final FixPConnection connection)
    {
        return new ILink3Parser((ILink3Connection)connection);
    }

    public ILink3Proxy makeProxy(
        final FixPMessageDissector fixPDissector,
        final ExclusivePublication publication,
        final EpochNanoClock epochNanoClock)
    {
        return new ILink3Proxy(this, 0, publication, null, epochNanoClock);
    }

    public ILink3Offsets makeOffsets()
    {
        return new ILink3Offsets();
    }

    public InternalFixPConnection makeAcceptorConnection(
        final long connectionId,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final FixPSessionOwner libraryPoller,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final long lastConnectPayload,
        final FixPContext context,
        final CommonConfiguration configuration, final FixPMessageDissector dissector)
    {
        return unsupported();
    }

    public Ilink3Storage makeStorage(final EpochNanoClock clock)
    {
        return new Ilink3Storage(clock);
    }

    public ILink3SequenceExtractor makeSequenceExtractor(
        final FixPSequenceNumberHandler handler,
        final SequenceNumberIndexReader sequenceNumberReader)
    {
        return new ILink3SequenceExtractor(
            handler,
            makeOffsets(),
            makeParser(null));
    }

    protected Ir loadIr()
    {
        return loadSbeIr();
    }

    public boolean explicitSequenceNumbers()
    {
        return true;
    }
}
