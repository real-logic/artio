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

import b3.entrypoint.fixp.sbe.*;
import b3.entrypoint.fixp.sbe.Boolean;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import uk.co.real_logic.artio.fixp.FixPRejectRefIdExtractor;
import uk.co.real_logic.sbe.ir.Ir;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointProxy.BINARY_ENTRYPOINT_HEADER_LENGTH;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.writeBinaryEntryPointSofh;

public class FixPRejectRefIdExtractorTest
{
    private static final long CL_ORD_ID = 123;
    private static final int OFFSET = 1;
    private static final int MESSAGE_SIZE = BINARY_ENTRYPOINT_HEADER_LENGTH + NewOrderSingleEncoder.BLOCK_LENGTH;

    @Test
    public void shouldExtractClOrdId()
    {
        final Ir ir = BinaryEntryPointProtocol.loadSbeIr();
        final FixPRejectRefIdExtractor rejectRefIdExtractor = new FixPRejectRefIdExtractor(ir);

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        writeNewOrderSingle(buffer);

        assertTrue(rejectRefIdExtractor.search(buffer, OFFSET));
        assertEquals(NewOrderSingleEncoder.TEMPLATE_ID, rejectRefIdExtractor.templateId());
        assertEquals(MessageType.NewOrderSingle.value(), (short)rejectRefIdExtractor.messageType());
        assertEquals(CL_ORD_ID, buffer.getLong(rejectRefIdExtractor.offset()));
        assertEquals(BitUtil.SIZE_OF_LONG, rejectRefIdExtractor.length());
    }

    private void writeNewOrderSingle(final UnsafeBuffer buffer)
    {
        final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
        final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();

        writeBinaryEntryPointSofh(buffer, OFFSET, MESSAGE_SIZE);

        newOrderSingle
            .wrapAndApplyHeader(buffer, OFFSET + SOFH_LENGTH, messageHeader)
            .clOrdID(CL_ORD_ID)
            .securityID(456)
            .price().mantissa(3);
        newOrderSingle
            .account(5)
            .marketSegmentID(NewOrderSingleEncoder.marketSegmentIDNullValue())
            .side(Side.BUY)
            .ordType(OrdType.MARKET)
            .timeInForce(TimeInForce.FILL_OR_KILL)
            .stopPx().mantissa(PriceOptionalEncoder.mantissaNullValue());
        newOrderSingle
            .enteringTrader("Maria")
            .ordTagID((short)1)
            .mmProtectionReset(Boolean.TRUE_VALUE)
            .routingInstruction(RoutingInstruction.NULL_VAL)
            .investorID(123)
            .custodianInfo()
            .custodian(1)
            .custodyAccount(2)
            .custodyAllocationType(3);
    }
}
