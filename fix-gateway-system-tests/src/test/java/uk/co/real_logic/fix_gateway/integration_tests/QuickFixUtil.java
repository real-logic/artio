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

import quickfix.*;
import quickfix.field.*;
import quickfix.fix44.Logon;
import quickfix.fix44.TestRequest;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public final class QuickFixUtil
{
    public static TestRequest testRequest()
    {
        final TestRequest message = new TestRequest(new TestReqID("abc"));
        setupHeader(message);
        return message;
    }

    public static void setupHeader(final Message message)
    {
        final Message.Header header = message.getHeader();
        header.setField(new SenderCompID("LEH_LZJ02"));
        header.setField(new TargetCompID("CCG"));
        header.setField(new MsgSeqNum(1));
        header.setField(new SendingTime(new Date(0)));
    }

    public static Logon logon()
    {
        final Logon message = new Logon();
        message.set(new HeartBtInt(10));
        setupHeader(message);
        return message;
    }

    public static void assertFieldEquals(
        final String expectedValue, final FieldMap decoder, final StringField field) throws FieldNotFound
    {
        decoder.getField(field);
        assertEquals(expectedValue, field.getValue());
    }

    public static void assertFieldEquals(
        final int expectedValue, final FieldMap decoder, final IntField field) throws FieldNotFound
    {
        decoder.getField(field);
        assertEquals(expectedValue, field.getValue());
    }
}
