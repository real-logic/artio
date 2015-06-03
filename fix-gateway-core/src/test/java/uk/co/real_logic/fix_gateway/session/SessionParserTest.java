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
package uk.co.real_logic.fix_gateway.session;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.auth.AuthenticationStrategy;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;

public class SessionParserTest
{

    private Session mockSession = mock(Session.class);
    private SessionIdStrategy mockSessionIdStrategy = mock(SessionIdStrategy.class);
    private SessionIds mockSessionIds = mock(SessionIds.class);
    private AuthenticationStrategy mockAuthenticationStrategy = mock(AuthenticationStrategy.class);

    private SessionParser parser = new SessionParser(
        mockSession, mockSessionIdStrategy, mockSessionIds, mockAuthenticationStrategy);

    @Test
    public void shouldNotifySessionOfMissingMsgSeqNum()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(
            "8=FIX.4.4\00135=B\00149=TW\00152=00000101-00:00:00.000\00156=ISLD\001112=TEST\001".getBytes(US_ASCII));

        parser.onMessage(buffer, 0, buffer.capacity(), 'B');

        verify(mockSession).onMessage(MISSING_INT);
    }

}
