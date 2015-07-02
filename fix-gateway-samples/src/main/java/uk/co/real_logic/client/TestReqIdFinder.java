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
package uk.co.real_logic.client;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static uk.co.real_logic.fix_gateway.decoder.Constants.TEST_REQ_ID;

public class TestReqIdFinder implements SessionHandler, OtfMessageAcceptor
{

    private final OtfParser parser = new OtfParser(this, new IntDictionary());
    final AsciiFlyweight string = new AsciiFlyweight();

    private String testReqId;

    public void onMessage(final DirectBuffer buffer,
                          final int offset,
                          final int length,
                          final long connectionId,
                          final long sessionId,
                          final int messageType)
    {
        // You can hook your own parsers at this point,
        // Here's an example using our otf parser

        parser.onMessage(buffer, offset, length);
    }

    public void onNext()
    {
    }

    public void onComplete()
    {
    }

    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        string.wrap(buffer);
        if (tag == TEST_REQ_ID)
        {
            this.testReqId = string.getAscii(offset, length);
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
