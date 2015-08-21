/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.server;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.builder.Printer;
import uk.co.real_logic.fix_gateway.decoder.PrinterImpl;
import uk.co.real_logic.fix_gateway.library.session.Session;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

public class SampleSessionHandler implements SessionHandler
{

    private final AsciiFlyweight string = new AsciiFlyweight();
    private final Printer printer = new PrinterImpl();

    public SampleSessionHandler(final Session session)
    {
    }

    public void onMessage(final DirectBuffer buffer,
                          final int offset,
                          final int length,
                          final long connectionId,
                          final long sessionId,
                          final int messageType, final long timestamp)
    {
        string.wrap(buffer);
        System.out.printf("%d -> %s\n", connectionId, printer.toString(string, offset, length, messageType));
    }

    public void onDisconnect(final long connectionId)
    {
        System.out.printf("%d Disconnected", connectionId);
    }
}
