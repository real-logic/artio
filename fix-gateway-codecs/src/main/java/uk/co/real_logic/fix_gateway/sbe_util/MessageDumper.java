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
package uk.co.real_logic.fix_gateway.sbe_util;

import org.agrona.DirectBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;

import java.nio.ByteBuffer;
import java.util.List;

public class MessageDumper
{
    private final Ir ir;

    public MessageDumper(final ByteBuffer schemaBuffer)
    {
        ir = new IrDecoder(schemaBuffer).decode();
    }

    public String toString(
        final int templateId,
        final int actingVersion,
        final int blockLength,
        final DirectBuffer buffer,
        final int offset)
    {
        final List<Token> tokens = ir.getMessage(templateId);

        final DumpingTokenListener tokenListener = new DumpingTokenListener();

        OtfMessageDecoder.decode(
            buffer,
            offset,
            actingVersion,
            blockLength,
            tokens,
            tokenListener);

        return tokenListener.toString();
    }
}
