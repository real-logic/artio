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
package uk.co.real_logic.artio.fixp;

import io.aeron.ExclusivePublication;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.sbe.CompositeDecoderFlyweight;
import org.agrona.sbe.MessageDecoderFlyweight;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.engine.logger.FixPSequenceNumberHandler;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.library.FixPSessionOwner;
import uk.co.real_logic.artio.library.InternalFixPConnection;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

// Implementation classes should be stateless
public abstract class FixPProtocol
{
    public static final boolean BUSINESS_MESSAGE_LOGGING_ENABLED = DebugLogger.isEnabled(LogTag.FIXP_BUSINESS);

    public static final int DOES_NOT_SUPPORT_SEQUENCE_FINISHING_TEMPLATE_ID = -1;

    private final String packageName;
    private final FixPProtocolType protocolType;
    private final short encodingType;
    private final int finishedSendingTemplateId;
    private final int finishedReceivingTemplateId;
    private final int negotiateResponseTemplateId;

    protected FixPProtocol(
        final FixPProtocolType protocolType,
        final short encodingType,
        final int negotiateResponseTemplateId,
        final String packageName)
    {
        this(
            protocolType,
            encodingType,
            DOES_NOT_SUPPORT_SEQUENCE_FINISHING_TEMPLATE_ID,
            DOES_NOT_SUPPORT_SEQUENCE_FINISHING_TEMPLATE_ID,
            negotiateResponseTemplateId,
            packageName);
    }

    protected FixPProtocol(
        final FixPProtocolType protocolType,
        final short encodingType,
        final int finishedSendingTemplateId,
        final int finishedReceivingTemplateId,
        final int negotiateResponseTemplateId,
        final String packageName)
    {
        this.packageName = packageName;
        this.protocolType = protocolType;
        this.encodingType = encodingType;
        this.finishedSendingTemplateId = finishedSendingTemplateId;
        this.finishedReceivingTemplateId = finishedReceivingTemplateId;
        this.negotiateResponseTemplateId = negotiateResponseTemplateId;
    }

    public FixPProtocolType protocolType()
    {
        return protocolType;
    }

    public short encodingType()
    {
        return encodingType;
    }

    public int finishedSendingTemplateId()
    {
        return finishedSendingTemplateId;
    }

    public int finishedReceivingTemplateId()
    {
        return finishedReceivingTemplateId;
    }

    public int negotiateResponseTemplateId()
    {
        return negotiateResponseTemplateId;
    }

    public abstract AbstractFixPParser makeParser(FixPConnection session);

    public abstract AbstractFixPProxy makeProxy(
        FixPMessageDissector fixPDissector, ExclusivePublication publication, EpochNanoClock epochNanoClock);

    public abstract AbstractFixPOffsets makeOffsets();

    public abstract InternalFixPConnection makeAcceptorConnection(
        long connectionId,
        GatewayPublication outboundPublication,
        GatewayPublication inboundPublication,
        int libraryId,
        FixPSessionOwner libraryPoller,
        long lastReceivedSequenceNumber,
        long lastSentSequenceNumber,
        long lastConnectPayload,
        FixPContext context,
        CommonConfiguration configuration,
        FixPMessageDissector dissector);

    public abstract AbstractFixPStorage makeStorage(
        EpochNanoClock epochNanoClock);

    public abstract AbstractFixPSequenceExtractor makeSequenceExtractor(
        FixPSequenceNumberHandler handler,
        SequenceNumberIndexReader sequenceNumberIndex);

    protected abstract Ir loadIr();

    public FixPRejectRefIdExtractor makeRefIdExtractor()
    {
        return new FixPRejectRefIdExtractor(loadIr());
    }

    /**
     * Gets whether the protocol puts an explicit sequence number in every message or not.
     *
     * @return true iff the protocol puts an explicit sequence number in every message.
     */
    public abstract boolean explicitSequenceNumbers();

    public List<? extends MessageDecoderFlyweight> messageDecoders()
    {
        return loadIr()
            .messages()
            .stream()
            .map(tokens ->
            {
                final Token beginMessage = tokens.get(0);
                final String decoderName = packageName + "." + beginMessage.name() + "Decoder";
                return (MessageDecoderFlyweight)newInstance(decoderName);
            })
            .collect(Collectors.toList());
    }

    public CompositeDecoderFlyweight makeHeader()
    {
        return (CompositeDecoderFlyweight)newInstance(packageName + ".MessageHeaderDecoder");
    }

    private Object newInstance(final String decoderName)
    {
        try
        {
            return Class.forName(decoderName).getConstructor().newInstance();
        }
        catch (final InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException |
            InvocationTargetException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }
}
