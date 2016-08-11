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
package uk.co.real_logic.fix_gateway.system_benchmarks;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.builder.Printer;
import uk.co.real_logic.fix_gateway.decoder.PrinterImpl;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public final class BenchmarkSessionHandler implements SessionHandler
{
    private final AsciiBuffer flyweight = new MutableAsciiBuffer();
    private final Printer printer = new PrinterImpl();

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long sessionId,
        final int messageType,
        final long timestampInNs,
        final long position)
    {
        //flyweight.wrap(buffer);
        //System.out.printf("Received Message: ");
        //System.out.println(printer.toString(flyweight, offset, length, messageType));
        return CONTINUE;
    }

    public void onTimeout(final int libraryId, final long sessionId)
    {
    }

    public Action onDisconnect(final int libraryId, final long sessionId, final DisconnectReason reason)
    {
        System.out.printf("%d disconnected\n", sessionId);

        return CONTINUE;
    }

}
