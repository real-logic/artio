/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class PasswordCleanerTest
{
    private static final String EXAMPLE_LOGON =
        "8=FIX.4.4\0019=099\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bob\001554=Uv1aegoh\00110=062\001";

    private static final String LONG_EXAMPLE_LOGON =
        "8=FIX.4.4\0019=102\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bob\001554=Uv1aegohABC\00110=062\001";

    private static final String EXPECTED_CLEANED_LOGON =
        "8=FIX.4.4\0019=094\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bob\001554=***\00110=062\001";

    private static final String NO_PASSWORD_LOGON =
        "8=FIX.4.4\0019=78\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\00110=062\001";

    private static final String EXAMPLE_LOGON_SHORT_PASSWORD =
        "8=FIX.4.4\0019=98\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bobdobsdobs\001554=x\00110=062\001";

    private static final String EXAMPLE_LOGON_EMPTY_PASSWORD =
        "8=FIX.4.4\0019=97\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bobdobsdobs\001554=\00110=062\001";

    private static final String EXPECTED_CLEANED_LOGON_SHORT_OR_EMPTY_PASSWORD =
        "8=FIX.4.4\0019=101\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bobdobsdobs\001554=***\00110=062\001";

    private static final String EXAMPLE_LOGON_SHORT_NAME_PASSWORD =
        "8=FIX.4.4\0019=94\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bobdobs\001554=x\00110=062\001";

    private static final String EXPECTED_CLEANED_LOGON_SHORT_NAME_PASSWORD =
        "8=FIX.4.4\0019=097\00135=A\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\00198=0\001108=10\001141=N\001553=bobdobs\001554=***\00110=062\001";

    private static final String EXAMPLE_USER_REQUEST =
        "8=FIX.4.4\0019=116\00135=BE\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\001923=A\001924=3\001553=initiator\001554=Uv1aegoh\001925=newPassword\00110=062\001";

    private static final String EXAMPLE_USER_REQUEST_SHORT_PASSWORD =
        "8=FIX.4.4\0019=98\00135=BE\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\001923=A\001924=3\001553=initiator\001554=O\001925=N\00110=062\001";

    private static final String CLEAN_USER_REQUEST =
        "8=FIX.4.4\0019=103\00135=BE\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\001923=A\001924=3\001553=initiator\001554=***\001925=***\00110=062\001";

    private static final String EXAMPLE_USER_REQUEST_FLIPPED_FIELD_ORDER =
        "8=FIX.4.4\0019=116\00135=BE\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\001923=A\001924=3\001553=initiator\001925=newPassword\001554=Uv1aegoh\00110=062\001";

    private static final String EXAMPLE_USER_REQUEST_FLIPPED_FIELD_ORDER_SHORT =
        "8=FIX.4.4\0019=98\00135=BE\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\001923=A\001924=3\001553=initiator\001925=N\001554=O\00110=062\001";

    private static final String CLEAN_USER_REQUEST_FLIPPED_FIELD_ORDER =
        "8=FIX.4.4\0019=103\00135=BE\00149=initiator\00156=acceptor\00134=1\00152=20191002-16:54:47.446" +
        "\001923=A\001924=3\001553=initiator\001925=***\001554=***\00110=062\001";

    private final PasswordCleaner passwordCleaner = new PasswordCleaner();

    private final int offset;

    public PasswordCleanerTest(final int offset)
    {
        this.offset = offset;
    }

    @Test
    public void shouldCleanPasswordFromLogon()
    {
        shouldCleanMessage(EXAMPLE_LOGON, EXPECTED_CLEANED_LOGON);
    }

    @Test
    public void shouldCleanLongPasswordFromLogon()
    {
        shouldCleanMessage(LONG_EXAMPLE_LOGON, EXPECTED_CLEANED_LOGON);
    }

    @Test
    public void shouldNotChangeLogonWithoutPassword()
    {
        shouldCleanMessage(NO_PASSWORD_LOGON, NO_PASSWORD_LOGON);
    }

    @Test
    public void shouldCleanPasswordFromLogonWithShortPassword()
    {
        shouldCleanMessage(EXAMPLE_LOGON_SHORT_PASSWORD, EXPECTED_CLEANED_LOGON_SHORT_OR_EMPTY_PASSWORD);
    }

    @Test
    public void shouldCleanPasswordFromLogonWithShortNamePassword()
    {
        shouldCleanMessage(EXAMPLE_LOGON_SHORT_NAME_PASSWORD, EXPECTED_CLEANED_LOGON_SHORT_NAME_PASSWORD);
    }

    @Test
    public void shouldCleanPasswordFromLogonWithEmptyPassword()
    {
        shouldCleanMessage(EXAMPLE_LOGON_EMPTY_PASSWORD, EXPECTED_CLEANED_LOGON_SHORT_OR_EMPTY_PASSWORD);
    }

    @Test
    public void shouldCleanPasswordsFromUserRequest()
    {
        shouldCleanMessage(EXAMPLE_USER_REQUEST, CLEAN_USER_REQUEST);
    }

    @Test
    public void shouldCleanPasswordsFromUserRequestShort()
    {
        shouldCleanMessage(EXAMPLE_USER_REQUEST_SHORT_PASSWORD, CLEAN_USER_REQUEST);
    }

    @Test
    public void shouldCleanPasswordsFromUserRequestWithFieldsFlipped()
    {
        shouldCleanMessage(EXAMPLE_USER_REQUEST_FLIPPED_FIELD_ORDER, CLEAN_USER_REQUEST_FLIPPED_FIELD_ORDER);
    }

    @Test
    public void shouldCleanPasswordsFromUserRequestWithFieldsFlippedShort()
    {
        shouldCleanMessage(EXAMPLE_USER_REQUEST_FLIPPED_FIELD_ORDER_SHORT, CLEAN_USER_REQUEST_FLIPPED_FIELD_ORDER);
    }

    private void shouldCleanMessage(final String inputMessage, final String expectedCleanedMessage)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final int length = buffer.putStringWithoutLengthAscii(offset, inputMessage);

        passwordCleaner.clean(buffer, offset, length);

        final DirectBuffer directBuffer = passwordCleaner.cleanedBuffer();
        final int cleanedLength = passwordCleaner.cleanedLength();
        final String cleanedLogon = directBuffer.getStringWithoutLengthAscii(0, cleanedLength);
        assertEquals(expectedCleanedMessage, cleanedLogon);
    }

    @Parameterized.Parameters(name = "offset={0}")
    public static Iterable<Object[]> decimalFloatCodecData()
    {
        return Arrays.asList(new Object[][]
            {
                {0},
                {100}
            });
    }
}
