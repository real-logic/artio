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
package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.util.ArrayList;
import java.util.List;

/**
 * An otf acceptor used to accumulate/log/check acceptor interactions.
 */
public class FakeOtfAcceptor implements OtfMessageAcceptor
{
    private final List<Integer> messageTypes = new ArrayList<>();
    private final AsciiFlyweight string = new AsciiFlyweight();

    private volatile boolean hasSeenMessage = false;

    public void onNext()
    {
        DebugLogger.log("Next Message");
    }

    public void onComplete()
    {
        hasSeenMessage = true;
        DebugLogger.log("Message Complete");
    }

    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        DebugLogger.log("Field: %s=%s\n", tag, buffer, offset, length);
        if (tag == 35)
        {
            string.wrap(buffer);
            messageTypes.add(string.getMessageType(offset, length));
        }
    }

    public void onGroupHeader(final int tag, final int numInGroup)
    {

    }

    public void onGroupBegin(final int tag, final int numInGroup, final int index)
    {

    }

    public void onGroupEnd(final int tag, final int numInGroup, final int index)
    {

    }

    public boolean onError(
        final ValidationError error,
        final int messageType,
        final int tagNumber,
        final AsciiFieldFlyweight value)
    {
        System.err.printf("%s for %d @ %d", error, messageType, tagNumber);
        return false;
    }

    public List<Integer> messageTypes()
    {
        return messageTypes;
    }

    public boolean hasSeenMessage()
    {
        return hasSeenMessage;
    }
}
