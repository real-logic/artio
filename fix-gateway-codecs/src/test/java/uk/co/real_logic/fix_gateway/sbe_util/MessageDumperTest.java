/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.messages.ManageConnectionEncoder;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ManageConnectionEncoder.TEMPLATE_ID;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;

public class MessageDumperTest
{
    @Test
    public void dumpsIr()
    {
        final int offset = 1;
        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final MessageDumper dumper = new MessageDumper(MessageSchemaIr.SCHEMA_BUFFER);
        final ManageConnectionEncoder encoder = new ManageConnectionEncoder();

        encoder
            .wrap(buffer, offset)
            .libraryId(4)
            .connection(1)
            .connectionType(ACCEPTOR)
            .lastSentSequenceNumber(3)
            .lastReceivedSequenceNumber(2)
            .sessionState(ACTIVE)
            .heartbeatIntervalInS(4)
            .replyToId(3)
            .sequenceIndex(5)
            .address("www.example.com:8000");

        final String result = dumper.toString(
            TEMPLATE_ID, encoder.sbeSchemaVersion(), encoder.sbeBlockLength(), buffer, offset);

        assertEquals(
            "{\n" +
            "    Template: 'ManageConnection',\n" +
            "    libraryId: 4,\n" +
            "    connection: 1,\n" +
            "    session: 0,\n" +
            "    connectionType: 'ACCEPTOR',\n" +
            "    lastSentSequenceNumber: 3,\n" +
            "    lastReceivedSequenceNumber: 2,\n" +
            "    sessionState: 'ACTIVE',\n" +
            "    heartbeatIntervalInS: 4,\n" +
            "    replyToId: 3,\n" +
            "    sequenceIndex: 5,\n" +
            "    address: 'www.example.com:8000'\n" +
            "}",
            result
        );
    }
}
