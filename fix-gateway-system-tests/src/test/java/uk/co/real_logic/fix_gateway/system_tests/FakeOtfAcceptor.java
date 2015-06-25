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
import uk.co.real_logic.fix_gateway.decoder.Constants;
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

    private ValidationError error;
    private boolean isCompleted;
    private String senderCompId;

    public void onNext()
    {
        DebugLogger.log("Next Message");
        senderCompId = null;
        error = null;
        isCompleted = false;
    }

    public void onComplete()
    {
        DebugLogger.log("Message Complete");
        isCompleted = true;
    }

    public synchronized void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        DebugLogger.log("Field: %s=%s\n", tag, buffer, offset, length);
        if (tag == Constants.MSG_TYPE)
        {
            string.wrap(buffer);
            messageTypes.add(string.getMessageType(offset, length));
        }
        else if (tag == Constants.SENDER_COMP_ID)
        {
            string.wrap(buffer);
            senderCompId = string.getAscii(offset, length);
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
        this.error = error;
        System.err.printf("%s for %d @ %d", error, messageType, tagNumber);
        return false;
    }

    public synchronized List<Integer> messageTypes()
    {
        return messageTypes;
    }

    public String lastSenderCompId()
    {
        return senderCompId;
    }

    public ValidationError lastError()
    {
        return error;
    }

    public boolean isCompleted()
    {
        return isCompleted;
    }
}
