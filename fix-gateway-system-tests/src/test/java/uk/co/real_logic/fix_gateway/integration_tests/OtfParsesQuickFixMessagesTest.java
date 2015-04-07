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
package uk.co.real_logic.fix_gateway.integration_tests;

import org.junit.Test;
import quickfix.Message;
import quickfix.field.TestReqID;
import quickfix.fix44.TestRequest;
import uk.co.real_logic.fix_gateway.DebugLogger;

public class OtfParsesQuickFixMessagesTest extends AbstractOtfParserTest
{
    @Test
    public void parseTestRequest()
    {
        final TestRequest message = new TestRequest(
            new TestReqID("abc")
        );

        final int length = encodeMessage(message);

        parseTestRequest(0, length);
    }

    private int encodeMessage(final Message message)
    {
        final String encodedMessage = message.toString();
        DebugLogger.log(encodedMessage);
        string.putAscii(0, encodedMessage);
        return encodedMessage.length();
    }
}
