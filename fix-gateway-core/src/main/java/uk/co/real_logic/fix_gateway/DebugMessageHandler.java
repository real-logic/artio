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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.builder.Printer;
import uk.co.real_logic.fix_gateway.decoder.PrinterImpl;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

public final class DebugMessageHandler
{

    private final Printer printer = new PrinterImpl();
    private final AsciiFlyweight string = new AsciiFlyweight();

    public DebugMessageHandler()
    {
    }

    public void onMessage(
        final DirectBuffer buffer, final int offset, final int length, final long sessionId, final int messageType)
    {
        string.wrap(buffer);
        final String message = printer.toString(string, offset, length, messageType);
        System.out.printf("Received from %d", sessionId);
        System.out.println(message);
        // TODO: delegate.onMessage(buffer, offset, length, sessionId, messageType);
    }
}
