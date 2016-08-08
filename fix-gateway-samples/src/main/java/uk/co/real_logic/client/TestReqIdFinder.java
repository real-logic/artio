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
package uk.co.real_logic.client;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.otf.MessageControl;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.decoder.Constants.TEST_REQ_ID;

public class TestReqIdFinder implements SessionHandler, OtfMessageAcceptor
{

    private final OtfParser parser = new OtfParser(this, new IntDictionary());

    private String testReqId;

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long sessionId,
        final int messageType,
        final long timestamp,
        final long position)
    {
        // You can hook your own parsers at this point,
        // Here's an example using our otf parser

        parser.onMessage(buffer, offset, length);

        return CONTINUE;
    }

    public void onTimeout(final int libraryId, final long sessionId)
    {
    }

    public Action onDisconnect(final int libraryId, final long sessionId, final DisconnectReason reason)
    {
        return CONTINUE;
    }

    public MessageControl onNext()
    {
        return MessageControl.CONTINUE;
    }

    public MessageControl onComplete()
    {
        return MessageControl.CONTINUE;
    }

    public MessageControl onField(final int tag, final AsciiBuffer buffer, final int offset, final int length)
    {
        if (tag == TEST_REQ_ID)
        {
            this.testReqId = buffer.getAscii(offset, length);
        }

        return MessageControl.CONTINUE;
    }

    public MessageControl onGroupHeader(final int tag, final int numInGroup)
    {
        return MessageControl.CONTINUE;
    }

    public MessageControl onGroupBegin(final int tag, final int numInGroup, final int index)
    {
        return MessageControl.CONTINUE;
    }

    public MessageControl onGroupEnd(final int tag, final int numInGroup, final int index)
    {
        return MessageControl.CONTINUE;
    }

    public boolean onError(final ValidationError error,
                           final int messageType,
                           final int tagNumber,
                           final AsciiFieldFlyweight value)
    {
        return false;
    }

    public String testReqId()
    {
        return testReqId;
    }
}
