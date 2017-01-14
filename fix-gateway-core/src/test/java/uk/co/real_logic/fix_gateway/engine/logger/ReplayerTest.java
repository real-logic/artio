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
package uk.co.real_logic.fix_gateway.engine.logger;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.IdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.decoder.ResendRequestDecoder;
import uk.co.real_logic.fix_gateway.engine.PossDupEnabler;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MOST_RECENT_MESSAGE;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.util.AsciiBuffer.UNKNOWN_INDEX;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.sequenceEqualsAscii;

public class ReplayerTest extends AbstractLogTest
{
    public static final byte[] MESSAGE_REQUIRING_LONGER_BODY_LENGTH =
        ("8=FIX.4.4\0019=99\00135=1\00134=1\00149=LEH_LZJ02\00152=19700101-00:00:00.000\00156=CCG\001" +
            "112=a12345678910123456789101234567891012345\00110=005\001").getBytes(US_ASCII);

    private static final int SIZE_OF_FRAME =
        FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength() + MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int MAX_CLAIM_ATTEMPTS = 100;

    private ReplayQuery replayQuery = mock(ReplayQuery.class);
    private ClusterableSubscription subscription = mock(ClusterableSubscription.class);
    private IdleStrategy idleStrategy = mock(IdleStrategy.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);

    private Replayer replayer = new Replayer(
        replayQuery,
        publication,
        claim,
        idleStrategy,
        errorHandler,
        MAX_CLAIM_ATTEMPTS,
        subscription,
        DEFAULT_NAME_PREFIX);

    @Before
    public void setUp()
    {
        when(publication.tryClaim(anyInt(), any())).thenReturn(1L);
        when(replayQuery.query(eq(replayer), anyLong(), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(1);
    }

    @Test
    public void shouldParseResendRequest()
    {
        final int length = bufferHasResendRequest(END_SEQ_NO);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, length);

        verifyQueriedService(END_SEQ_NO);
        verifyNoMoreInteractions(publication);
    }

    @Test
    public void shouldPublishAllRemainingMessages()
    {
        final int length = bufferHasResendRequest(MOST_RECENT_MESSAGE);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, length);

        verifyQueriedService(MOST_RECENT_MESSAGE);
        verifyNoMoreInteractions(publication);
    }

    @Test
    public void shouldPublishMessagesWithSetPossDupFlag()
    {
        bufferContainsMessage(true);
        final int srcLength = setupMessage();

        replayer.onFragment(buffer, START, srcLength, null);

        verifyClaim(srcLength);
        assertHasSetPossDupFlag();
        verifyCommit();
    }

    @Test
    public void shouldReplayMessageWithExpandingBodyLength()
    {
        bufferContainsMessage(MESSAGE_REQUIRING_LONGER_BODY_LENGTH);
        final int srcLength = setupMessage();

        replayer.onFragment(buffer, START, srcLength, null);

        verifyClaim(srcLength + 6);
        assertHasSetPossDupFlag();
        verifyCommit();
        hasNotOverwrittenSeperatorChar();
    }

    private void hasNotOverwrittenSeperatorChar()
    {
        final String lengthSection = resultAsciiBuffer.getAscii(offset + 11, 11);
        assertEquals("9=104\00135=1\001", lengthSection);
    }

    @Test
    public void shouldPublishMessagesWithoutSetPossDupFlag()
    {
        bufferContainsMessage(false);
        final int srcLength = setupMessage();

        replayer.onFragment(buffer, START, SIZE_OF_FRAME + srcLength, null);

        verifyClaim(SIZE_OF_FRAME + srcLength + PossDupEnabler.POSS_DUP_FIELD.length);
        assertHasSetPossDupFlag();
        verifyCommit();

        assertThat(resultAsciiBuffer,
            sequenceEqualsAscii("8=FIX.4.4\0019=0068\001", offset + 1));
    }

    @Test
    public void shouldIgnoreIrrelevantFixMessages()
    {
        onMessage(LogonDecoder.MESSAGE_TYPE, buffer.capacity());

        verifyNoMoreInteractions(replayQuery, publication);
    }

    @Test
    public void shouldValidateResendRequestMessageSequenceNumbers()
    {
        final int length = bufferHasResendRequest(BEGIN_SEQ_NO - 1);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, length);

        verify(errorHandler).onError(any());
        verifyNoMoreInteractions(replayQuery, publication);
    }

    @After
    public void shouldHaveNoMoreErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    private int setupMessage()
    {
        final int srcLength = fragmentLength();
        setupClaim(srcLength);
        setupPublication(srcLength);
        return srcLength;
    }

    private void verifyQueriedService(final int endSeqNo)
    {
        verify(replayQuery).query(replayer, SESSION_ID, BEGIN_SEQ_NO, SEQUENCE_INDEX, endSeqNo, SEQUENCE_INDEX);
    }

    private void assertHasSetPossDupFlag()
    {
        final int possDupIndex = resultAsciiBuffer.scan(0, resultBuffer.capacity(), 'Y');
        assertNotEquals("Unable to find poss dup index", UNKNOWN_INDEX, possDupIndex);
    }

    private void onMessage(final int messageType, final int length)
    {
        replayer.onMessage(
            buffer, ENCODE_OFFSET, length,
            LIBRARY_ID, CONNECTION_ID, SESSION_ID, SEQUENCE_INDEX, messageType, 0L, OK, 0L);
    }

    private void bufferContainsMessage(final byte[] message)
    {
        logEntryLength = message.length;
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(message);
        bufferContainsMessage(SESSION_ID, SEQUENCE_NUMBER, asciiBuffer);
    }
}
