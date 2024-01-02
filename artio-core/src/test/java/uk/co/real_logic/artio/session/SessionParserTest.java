/*
 * Copyright 2015-2024 Real Logic Limited., Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.session;

import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.messages.CancelOnDisconnectOption;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.Constants.TARGET_COMP_ID;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;

public class SessionParserTest
{
    private static final long POSITION = 64;
    private final Session mockSession = mock(Session.class);
    private final AuthenticationStrategy mockAuthenticationStrategy = mock(AuthenticationStrategy.class);
    private final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId("das");
    private final OnMessageInfo messageInfo = mock(OnMessageInfo.class);
    private final AbstractLogonDecoder logon = mock(AbstractLogonDecoder.class);

    private final SessionParser parser = new SessionParser(
        mockSession, validationStrategy, LangUtil::rethrowUnchecked,
        false, true, messageInfo, null);

    @Before
    public void setUp()
    {
        parser.fixDictionary(FixDictionary.of(FixDictionary.findDefault()));

        when(mockAuthenticationStrategy.authenticate(any(LogonDecoder.class))).thenReturn(true);
        when(mockSession.onBeginString(any(), anyInt(), anyBoolean())).thenReturn(true);
    }

    @Test
    public void shouldNotifySessionOfMissingMsgSeqNum()
    {
        final UnsafeBuffer buffer = bufferOf(
            "8=FIX.4.4\00135=B\00149=abc\00152=00000101-00:00:00.000\00156=das\001");

        parser.onMessage(buffer, 0, buffer.capacity(), 'B', POSITION);

        verify(mockSession).onMessage(
            eq(MISSING_INT), any(), anyInt(), anyLong(), anyLong(), eq(false), eq(false), eq(POSITION));
    }

    @Test
    public void shouldNotifySessionOfUnknownMessageType()
    {
        final UnsafeBuffer buffer = bufferOf(
            "8=FIX.4.4\00135=*\00134=2\00149=abc\00152=00000101-00:00:00.000\00156=das\001");

        parser.onMessage(buffer, 0, buffer.capacity(), '*', POSITION);

        verify(mockSession).onInvalidMessageType(eq(2), any(char[].class), anyInt(), eq(POSITION));
    }

    @Test
    public void shouldValidateCompId()
    {
        final UnsafeBuffer buffer = bufferOf(
            "8=FIX.4.2\0019=146\00135=D\00134=4\00149=WRONG\001" +
            "52=20090323-15:40:29\00156=WRONG\001115=XYZ\00111=NF 0542/03232009\00154=1\00138=100\001" +
            "55=CVS\00140=1\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=195\001");

        when(mockSession.state()).thenReturn(SessionState.AWAITING_LOGOUT);

        parser.onMessage(buffer, 0, buffer.capacity(), 'D', POSITION);

        verify(mockSession).onInvalidMessage(
            4,
            TARGET_COMP_ID,
            "D".toCharArray(),
            "D".length(),
            RejectReason.COMPID_PROBLEM.representation(), POSITION);

        verify(mockSession).startLogout();
        verify(mockSession, never()).onInvalidMessageType(anyInt(), any(), anyInt(), eq(POSITION));
    }

    @Test
    public void shouldGetCancelOnDisconnectFromMessage()
    {
        when(logon.hasCancelOnDisconnectType()).thenReturn(true);
        when(logon.supportsCancelOnDisconnectType()).thenReturn(true);
        when(logon.cancelOnDisconnectType()).thenReturn(CancelOnDisconnectOption.CANCEL_ON_DISCONNECT_ONLY.value());

        final CancelOnDisconnectOption option = SessionParser.cancelOnDisconnectType(logon,
            CancelOnDisconnectOption.DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT);

        assertEquals(CancelOnDisconnectOption.CANCEL_ON_DISCONNECT_ONLY, option);
    }

    @Test
    public void shouldGetDefaultCancelOnDisconnectWhenNotSupported()
    {
        when(logon.supportsCancelOnDisconnectType()).thenReturn(false);

        final CancelOnDisconnectOption option = SessionParser.cancelOnDisconnectType(logon,
            CancelOnDisconnectOption.DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT);

        assertEquals(CancelOnDisconnectOption.DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT, option);
    }

    @Test
    public void shouldGetDefaultCancelOnDisconnectWhenNotSet()
    {
        when(logon.supportsCancelOnDisconnectType()).thenReturn(true);
        when(logon.hasCancelOnDisconnectType()).thenReturn(false);

        final CancelOnDisconnectOption option = SessionParser.cancelOnDisconnectType(logon,
            CancelOnDisconnectOption.DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT);

        assertEquals(CancelOnDisconnectOption.DO_NOT_CANCEL_ON_DISCONNECT_OR_LOGOUT, option);
    }

    @Test
    public void shouldGetCancelOnDisconnectWindow()
    {
        when(logon.supportsCODTimeoutWindow()).thenReturn(true);
        when(logon.hasCODTimeoutWindow()).thenReturn(true);
        when(logon.cODTimeoutWindow()).thenReturn(20);

        final long codTimeoutInMs = SessionParser.cancelOnDisconnectTimeoutWindow(logon,
            10);

        assertEquals(20, codTimeoutInMs);
    }

    @Test
    public void shouldGetDefaultCancelOnDisconnectWindowWhenNotSupported()
    {
        when(logon.supportsCODTimeoutWindow()).thenReturn(false);
        when(logon.hasCODTimeoutWindow()).thenReturn(false);

        final long codTimeoutInMs = SessionParser.cancelOnDisconnectTimeoutWindow(logon,
            10);

        assertEquals(10, codTimeoutInMs);
    }

    @Test
    public void shouldGetDefaultCancelOnDisconnectWindowWhenNotSet()
    {
        when(logon.supportsCODTimeoutWindow()).thenReturn(true);
        when(logon.hasCODTimeoutWindow()).thenReturn(false);

        final long codTimeoutInMs = SessionParser.cancelOnDisconnectTimeoutWindow(logon,
            10);

        assertEquals(10, codTimeoutInMs);
    }

    private UnsafeBuffer bufferOf(final String str)
    {
        return new UnsafeBuffer(str.getBytes(US_ASCII));
    }
}
