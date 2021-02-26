/*
 * Copyright 2020 Monotonic Ltd.
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

import iLinkBinary.NewOrderSingle514Decoder;
import iLinkBinary.NotApplied513Decoder;
import iLinkBinary.PartyDetailsDefinitionRequestAck519Decoder;
import org.junit.Test;
import uk.co.real_logic.artio.fixp.AbstractFixPOffsets;

import static org.junit.Assert.assertEquals;

public class ILink3OffsetsTest
{

    private final AbstractFixPOffsets offsets = new ILink3Offsets();

    @Test
    public void shouldLoadSeqNumOffset()
    {
        assertEquals(NewOrderSingle514Decoder.seqNumEncodingOffset(),
            offsets.seqNumOffset(NewOrderSingle514Decoder.TEMPLATE_ID));
    }

    @Test
    public void shouldSupportMissingSeqNumOffset()
    {
        assertEquals(
            ILink3Offsets.MISSING_OFFSET,
            offsets.seqNumOffset(NotApplied513Decoder.TEMPLATE_ID));
    }

    @Test
    public void shouldLoadPossRetransOffset()
    {
        assertEquals(PartyDetailsDefinitionRequestAck519Decoder.possRetransFlagEncodingOffset(),
            offsets.possRetransOffset(PartyDetailsDefinitionRequestAck519Decoder.TEMPLATE_ID));
    }

    @Test
    public void shouldSupportMissingRetransOffset()
    {
        assertEquals(
            ILink3Offsets.MISSING_OFFSET,
            offsets.possRetransOffset(NewOrderSingle514Decoder.TEMPLATE_ID));
    }

    @Test
    public void shouldLoadSendingTimeOffset()
    {
        assertEquals(PartyDetailsDefinitionRequestAck519Decoder.sendingTimeEpochEncodingOffset(),
            offsets.sendingTimeEpochOffset(PartyDetailsDefinitionRequestAck519Decoder.TEMPLATE_ID));
    }

    @Test
    public void shouldSupportMissingSendingTimeOffset()
    {
        assertEquals(
            ILink3Offsets.MISSING_OFFSET,
            offsets.sendingTimeEpochOffset(NotApplied513Decoder.TEMPLATE_ID));
    }
}
