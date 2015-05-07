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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.logger.Replayer.SIZE_OF_LENGTH_FIELD;

public class AbstractMessageTest
{
    protected static final long SESSION_ID = 1;
    protected static final long SESSION_ID_2 = 2;
    protected static final int STREAM_ID = 2;
    protected static final int CONNECTION_ID = 1;
    protected static final int START = 1;
    protected static final int SEQUENCE_NUMBER = 5;

    protected MessageHeaderEncoder header = new MessageHeaderEncoder();
    protected FixMessageEncoder messageFrame = new FixMessageEncoder();

    protected UnsafeBuffer buffer = new UnsafeBuffer(new byte[16 * 1024]);

    protected int logEntryLength;
    protected int offset;

    protected void bufferContainsMessage(final boolean hasPossDupFlag)
    {
        bufferContainsMessage(hasPossDupFlag, SESSION_ID);
    }

    protected void bufferContainsMessage(final boolean hasPossDupFlag, final long sessionId)
    {
        final UnsafeBuffer msgBuffer = new UnsafeBuffer(new byte[8 * 1024]);
        final MutableAsciiFlyweight asciiFlyweight = new MutableAsciiFlyweight(msgBuffer);
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("abc");
        if (hasPossDupFlag)
        {
            testRequest
                .header()
                .possDupFlag(false)
                .msgSeqNum(SEQUENCE_NUMBER);
        }
        logEntryLength = testRequest.encode(asciiFlyweight, 0);

        offset = START;
        header
            .wrap(buffer, offset, 0)
            .blockLength(messageFrame.sbeBlockLength())
            .templateId(messageFrame.sbeTemplateId())
            .schemaId(messageFrame.sbeSchemaId())
            .version(messageFrame.sbeSchemaVersion());

        offset += header.size();

        messageFrame
            .wrap(buffer, offset)
            .messageType(TestRequestDecoder.MESSAGE_TYPE)
            .session(sessionId)
            .connection(CONNECTION_ID)
            .putBody(msgBuffer, 0, logEntryLength);

        offset += messageFrame.sbeBlockLength() + SIZE_OF_LENGTH_FIELD;
    }

    protected int messageLength()
    {
        return offset + logEntryLength - START;
    }
}
