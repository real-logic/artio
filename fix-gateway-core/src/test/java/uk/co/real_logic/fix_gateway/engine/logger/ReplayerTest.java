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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.IdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.fields.RejectReason;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.fix_gateway.engine.PossDupEnabler.POSS_DUP_FIELD;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MESSAGE_FRAME_BLOCK_LENGTH;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MOST_RECENT_MESSAGE;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.util.AsciiBuffer.UNKNOWN_INDEX;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.sequenceEqualsAscii;

public class ReplayerTest extends AbstractLogTest
{
    public static final byte[] MESSAGE_REQUIRING_LONGER_BODY_LENGTH =
        ("8=FIX.4.4\0019=99\00135=1\00134=1\00149=LEH_LZJ02\00152=19700101-00:00:00.000\00156=CCG\001" +
            "112=a12345678910123456789101234567891012345\00110=005\001").getBytes(US_ASCII);

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
        whenReplayQueried().thenReturn(1);
    }

    private OngoingStubbing<Integer> whenReplayQueried()
    {
        return when(replayQuery.query(eq(replayer), anyLong(), anyInt(), anyInt(), anyInt(), anyInt()));
    }

    @Test
    public void shouldParseResendRequest()
    {
        final long result = bufferHasResendRequest(END_SEQ_NO);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result);

        verifyQueriedService(END_SEQ_NO);
        verifyNoMoreInteractions(publication);
    }

    @Test
    public void shouldPublishAllRemainingMessages()
    {
        final long result = bufferHasResendRequest(MOST_RECENT_MESSAGE);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result);

        verifyQueriedService(MOST_RECENT_MESSAGE);
        verifyNoMoreInteractions(publication);
    }

    @Test
    public void shouldPublishMessagesWithSetPossDupFlag()
    {
        uponReplay(END_SEQ_NO, inv ->
        {
            bufferContainsExampleMessage(true);

            final int srcLength = fragmentLength();
            setupMessage(srcLength);

            onFragment(srcLength);

            assertHasResentWithPossDupFlag(srcLength, times(1));

            return 1;
        });
    }

    @Test
    public void shouldGapFillAdminMessages()
    {
        final int offset = setupCapturingClaim();
        uponReplay(END_SEQ_NO, inv ->
        {
            onTestRequest(SEQUENCE_NUMBER);

            return 1;
        });

        assertReSentGapFill(END_SEQ_NO, offset, times(1));
    }

    @Test
    public void shouldGapFillOnceForTwoConsecutiveAdminMessages()
    {
        final int endSeqNo = replayTwoMessages();
        final int offset = setupCapturingClaim();

        uponReplay(endSeqNo, inv ->
        {
            onTestRequest(SEQUENCE_NUMBER);

            onTestRequest(SEQUENCE_NUMBER + 1);

            return 2;
        });

        assertReSentGapFill(endSeqNo, offset, times(1));
    }

    @Test
    public void shouldGapFillForAdminMessagesFollowedByAppMessage()
    {
        final int endSeqNo = replayTwoMessages();
        final int offset = setupCapturingClaim();

        uponReplay(endSeqNo, inv ->
        {
            onTestRequest(BEGIN_SEQ_NO);

            final int srcLength = onExampleMessage(endSeqNo);

            assertResentGapFillThenMessage(endSeqNo, offset, srcLength);

            return 2;
        });
    }

    @Test
    public void shouldGapFillMissingMesages()
    {
        final int endSeqNo = replayTwoMessages();

        final int offset = setupCapturingClaim();
        whenReplayQueried().thenReturn(0);

        final long result = bufferHasResendRequest(endSeqNo);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result);

        assertReSentGapFill(endSeqNo + 1, offset, times(1));
        verifyIllegalStateException();
    }

    @Test
    public void shouldGapFillMissingMesagesFollowedByApplicationMessage()
    {
        final int endSeqNo = replayTwoMessages();

        uponReplay(endSeqNo, inv ->
        {
            final int offset = setupCapturingClaim();

            final int srcLength = onExampleMessage(endSeqNo);

            assertResentGapFillThenMessage(BEGIN_SEQ_NO, offset, srcLength);

            return 1;
        });

        verifyIllegalStateException();
    }

    // TODO: implications of back pressure
    //          failure to commit the gapfill (retry gapfill on abort?)
    //          failure to commit the messages

    @Test
    public void shouldReplayMessageWithExpandingBodyLength()
    {
        uponReplay(END_SEQ_NO, inv ->
        {
            bufferContainsMessage(MESSAGE_REQUIRING_LONGER_BODY_LENGTH);

            final int srcLength = fragmentLength();
            // Poss Dup Flag, and 1 longer body length
            final int newLength = srcLength + 6;
            setupMessage(newLength);

            onFragment(srcLength);

            assertHasResentWithPossDupFlag(newLength, times(1));
            hasNotOverwrittenSeperatorChar();

            assertEndsWithValidChecksum(offset + 1);

            return 1;
        });
    }

    @Test
    public void shouldReplayMessageWithExpandingBodyLengthWhenBackPressured()
    {
        uponReplay(END_SEQ_NO, ABORT, inv ->
        {
            bufferContainsMessage(MESSAGE_REQUIRING_LONGER_BODY_LENGTH);

            backpressureTryClaim();

            onFragment(fragmentLength());

            verifyClaim();

            return 1;
        });

        verifyNoMoreInteractions(publication, claim);
        reset(publication, claim);

        shouldReplayMessageWithExpandingBodyLength();
    }

    @Test
    public void shouldPublishMessagesWithoutSetPossDupFlag()
    {
        uponReplay(END_SEQ_NO, inv ->
        {
            bufferContainsExampleMessage(false);
            final int srcLength = fragmentLength();
            final int lengthAfterPossDupFlag = srcLength + POSS_DUP_FIELD.length;
            setupMessage(lengthAfterPossDupFlag);

            onFragment(srcLength);

            assertHasResentWithPossDupFlag(lengthAfterPossDupFlag, times(1));

            final int afterOffset = this.offset + 1;
            assertThat(resultAsciiBuffer,
                sequenceEqualsAscii("8=FIX.4.4\0019=68\001", afterOffset));

            assertThat(resultAsciiBuffer,
                sequenceEqualsAscii("8=FIX.4.4\0019=68\001", afterOffset));

            assertEndsWithValidChecksum(afterOffset);

            return 1;
        });
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
        final long result = bufferHasResendRequest(BEGIN_SEQ_NO - 1);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result);

        verify(errorHandler).onError(any());
        verifyNoMoreInteractions(replayQuery, publication);
    }

    @After
    public void shouldHaveNoMoreErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    private void assertResentGapFillThenMessage(final int endSeqNo, final int offset, final int srcLength)
    {
        doAnswer(commitInv ->
        {
            assertReSentGapFill(endSeqNo, offset, times(2));
            return null;
        }).when(claim).commit();

        assertHasResentWithPossDupFlag(srcLength, times(2));
    }

    private int onExampleMessage(final int endSeqNo)
    {
        bufferContainsExampleMessage(true, SESSION_ID, endSeqNo, SEQUENCE_INDEX);
        final int srcLength = fragmentLength();
        onFragment(srcLength);
        return srcLength;
    }

    private void onTestRequest(final int sequenceNumber)
    {
        bufferContainsTestRequest(sequenceNumber);
        onFragment(fragmentLength());
    }

    private int replayTwoMessages()
    {
        // inclusive numbering
        return BEGIN_SEQ_NO + 1;
    }

    private void onFragment(final int length)
    {
        replayer.onFragment(buffer, START, length, null);
    }

    private void uponReplay(final int endSeqNo, final Answer<?> answer)
    {
        uponReplay(endSeqNo, Action.CONTINUE, answer);
    }

    private void uponReplay(
        final int endSeqNo,
        final Action expectedAction,
        final Answer<?> answer)
    {
        whenReplayQueried().then(answer);

        final long result = bufferHasResendRequest(endSeqNo);
        final Action action = onMessage(ResendRequestDecoder.MESSAGE_TYPE, result);
        assertEquals(expectedAction, action);
    }

    private void verifyIllegalStateException()
    {
        verify(errorHandler).onError(any(IllegalStateException.class));
    }

    private void assertHasResentWithPossDupFlag(final int srcLength, final VerificationMode times)
    {
        verifyClaim(srcLength);
        assertResultBufferHasSetPossDupFlag();
        verifyCommit(times);
    }

    private void assertReSentGapFill(final int newSeqNo, final int offset, final VerificationMode times)
    {
        verifyClaim();
        assertResultBufferHasGapFillMessage(resultBuffer.capacity() - offset, newSeqNo);
        verifyCommit(times);
    }

    private void verifyClaim()
    {
        verify(publication, atLeastOnce()).tryClaim(anyInt(), eq(claim));
    }

    private void assertEndsWithValidChecksum(final int afterOffset)
    {
        final String message = resultAsciiBuffer.getAscii(afterOffset, resultAsciiBuffer.capacity() - afterOffset);
        final Matcher matcher = Pattern.compile("10=\\d+\001").matcher(message);
        assertTrue(message, matcher.find());
    }

    private void hasNotOverwrittenSeperatorChar()
    {
        final String lengthSection = resultAsciiBuffer.getAscii(offset + 11, 11);
        assertEquals("9=104\00135=1\001", lengthSection);
    }

    private void assertResultBufferHasGapFillMessage(final int claimedLength, final int newSeqNo)
    {
        final int offset = offset() + MESSAGE_FRAME_BLOCK_LENGTH;
        final int length = claimedLength - MESSAGE_FRAME_BLOCK_LENGTH;
        final String message = resultAsciiBuffer.getAscii(offset, length);
        final SequenceResetDecoder sequenceReset = new SequenceResetDecoder();
        sequenceReset.decode(resultAsciiBuffer, offset, length);
        final HeaderDecoder header = sequenceReset.header();

        if (!sequenceReset.validate())
        {
            fail(message + "\n" + sequenceReset.invalidTagId() + " " +
                RejectReason.decode(sequenceReset.rejectReason()));
        }

        assertTrue(message, sequenceReset.gapFillFlag());
        assertEquals(message, SEQUENCE_NUMBER, header.msgSeqNum());
        assertEquals(newSeqNo, sequenceReset.newSeqNo());
        assertTrue(message, header.possDupFlag());
    }

    private void setupMessage(final int length)
    {
        setupClaim(length);
        setupPublication(length);
    }

    private void verifyQueriedService(final int endSeqNo)
    {
        verify(replayQuery).query(replayer, SESSION_ID, BEGIN_SEQ_NO, SEQUENCE_INDEX, endSeqNo, SEQUENCE_INDEX);
    }

    private void assertResultBufferHasSetPossDupFlag()
    {
        final int possDupIndex = resultAsciiBuffer.scan(0, resultAsciiBuffer.capacity() - 1, 'Y');
        assertNotEquals("Unable to find poss dup index", UNKNOWN_INDEX, possDupIndex);
    }

    private Action onMessage(final int messageType, final long result)
    {
        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);
        return replayer.onMessage(
            buffer, offset, length,
            LIBRARY_ID, CONNECTION_ID, SESSION_ID, SEQUENCE_INDEX, messageType, 0L, OK, 0L);
    }

    private void bufferContainsMessage(final byte[] message)
    {
        logEntryLength = message.length;
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(message);
        bufferContainsMessage(SESSION_ID, SEQUENCE_NUMBER, asciiBuffer, ExampleMessageDecoder.MESSAGE_TYPE);
    }
}
