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
package uk.co.real_logic.fix_gateway.library.session;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.library.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;

public class SessionParserTest
{

    private Session mockSession = mock(Session.class);
    private SessionIdStrategy mockSessionIdStrategy = mock(SessionIdStrategy.class);
    private AuthenticationStrategy mockAuthenticationStrategy = mock(AuthenticationStrategy.class);

    private SessionParser parser = new SessionParser(
        mockSession, mockSessionIdStrategy, mockAuthenticationStrategy);

    @Before
    public void setUp()
    {
        when(mockAuthenticationStrategy.authenticate(any(LogonDecoder.class))).thenReturn(true);
    }

    @Test
    public void shouldNotifySessionOfMissingMsgSeqNum()
    {
        final UnsafeBuffer buffer = bufferOf(
            "8=FIX.4.4\00135=B\00149=abc\00152=00000101-00:00:00.000\00156=dsa\001");

        parser.onMessage(buffer, 0, buffer.capacity(), 'B', 1);

        verify(mockSession).onMessage(eq(MISSING_INT), any(), anyInt(), anyLong(), anyLong(), eq(false));
    }

    @Test
    public void shouldNotifySessionOfUnknownMessageType()
    {
        final UnsafeBuffer buffer = bufferOf(
            "8=FIX.4.4\00135=*\00134=2\00149=abc\00152=00000101-00:00:00.000\00156=das\001");

        parser.onMessage(buffer, 0, buffer.capacity(), '*', 1);

        verify(mockSession).onInvalidMessageType(eq(2), any(char[].class), anyInt());
    }

    private UnsafeBuffer bufferOf(final String str)
    {
        return new UnsafeBuffer(str.getBytes(US_ASCII));
    }

}
