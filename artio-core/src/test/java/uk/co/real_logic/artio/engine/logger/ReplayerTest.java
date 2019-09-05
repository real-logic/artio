/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Subscription;
import io.aeron.driver.Configuration;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.fields.UtcTimestampDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.ReplayCompleteDecoder;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.artio.decoder.ExampleMessageDecoder.MESSAGE_TYPE;
import static uk.co.real_logic.artio.engine.PossDupEnabler.ORIG_SENDING_TIME_PREFIX_AS_STR;
import static uk.co.real_logic.artio.engine.logger.Replayer.MESSAGE_FRAME_BLOCK_LENGTH;
import static uk.co.real_logic.artio.engine.logger.Replayer.MOST_RECENT_MESSAGE;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;
import static uk.co.real_logic.artio.util.CustomMatchers.sequenceEqualsAscii;

public class ReplayerTest extends AbstractLogTest
{
    private static final String DATE_TIME_STR = "19840521-15:00:00";
    private static final long DATE_TIME_EPOCH_MS =
        new UtcTimestampDecoder().decode(DATE_TIME_STR.getBytes(US_ASCII));

    public static final byte[] MESSAGE_REQUIRING_LONGER_BODY_LENGTH =
        ("8=FIX.4.4\0019=99\00135=1\00134=1\00149=LEH_LZJ02\00152=" + ORIGINAL_SENDING_TIME + "\00156=CCG\001" +
            "112=a12345678910123456789101234567891012345\00110=005\001").getBytes(US_ASCII);

    private static final int MAX_CLAIM_ATTEMPTS = 100;

    private ReplayQuery replayQuery = mock(ReplayQuery.class);
    private Subscription subscription = mock(Subscription.class);
    private IdleStrategy idleStrategy = mock(IdleStrategy.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private EpochClock clock = mock(EpochClock.class);
    private ArgumentCaptor<ControlledFragmentHandler> handler =
        ArgumentCaptor.forClass(ControlledFragmentHandler.class);
    private Header fragmentHeader = mock(Header.class);
    private ReplayHandler replayHandler = mock(ReplayHandler.class);
    private SenderSequenceNumbers senderSequenceNumbers = mock(SenderSequenceNumbers.class);
    private ReplayOperation replayOperation = mock(ReplayOperation.class);

    private Replayer replayer;

    @Before
    public void setUp()
    {
        when(fragmentHeader.flags()).thenReturn((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        when(clock.time()).thenReturn(DATE_TIME_EPOCH_MS);
        when(publication.tryClaim(anyInt(), any())).thenReturn(1L);
        when(publication.maxPayloadLength()).thenReturn(Configuration.mtuLength() - DataHeaderFlyweight.HEADER_LENGTH);

        when(replayQuery.query(handler.capture(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), any()))
            .thenReturn(replayOperation);
        when(replayOperation.attemptReplay()).thenReturn(true);

        setReplayedMessages(1);

        replayer = new Replayer(
            replayQuery,
            publication,
            claim,
            idleStrategy,
            errorHandler,
            MAX_CLAIM_ATTEMPTS,
            subscription,
            DEFAULT_NAME_PREFIX,
            clock,
            EngineConfiguration.DEFAULT_GAPFILL_ON_REPLAY_MESSAGE_TYPES,
            replayHandler,
            senderSequenceNumbers,
            new FakeFixSessionCodecsFactory());
    }

    private void setReplayedMessages(final int replayedMessages)
    {
        when(replayOperation.replayedMessages()).thenReturn(replayedMessages);
    }

    private OngoingStubbing<Boolean> whenReplayQueried()
    {
        return when(replayOperation.attemptReplay());
    }

    @Test
    public void shouldParseResendRequest()
    {
        final long result = bufferHasResendRequest(END_SEQ_NO);
        onRequestResendMessage(result);

        verifyQueriedService(END_SEQ_NO);
        verifyPublicationOnlyPayloadQueried();
    }

    @Test
    public void shouldPublishAllRemainingMessages()
    {
        final long result = bufferHasResendRequest(MOST_RECENT_MESSAGE);
        onRequestResendMessage(result);

        verifyQueriedService(MOST_RECENT_MESSAGE);
        verifyPublicationOnlyPayloadQueried();
    }

    @Test
    public void shouldPublishMessagesWithSetPossDupFlag()
    {
        onReplay(END_SEQ_NO, inv -> true);

        bufferContainsExampleMessage(true);

        final int srcLength = fragmentLength();
        setupMessage(srcLength);

        onFragment(srcLength);

        assertHasResentWithPossDupFlag(srcLength, times(1));

        replayer.doWork();
        replayer.doWork();

        verifyReplayCompleteMessageSent();
    }

    @Test
    public void shouldSupportConcurrentReplayRequests()
    {
        onReplay(END_SEQ_NO, inv -> true);

        onReplayOtherSession(END_SEQ_NO);

        // Two queries means two handlers
        final List<ControlledFragmentHandler> handlers = handler.getAllValues();
        assertEquals(2, handlers.size());
        final ControlledFragmentHandler firstHandler = handlers.get(0);
        final ControlledFragmentHandler secondHandler = handlers.get(1);
        assertNotSame(firstHandler, secondHandler);

        // First replay
        bufferContainsExampleMessage(true);
        final int srcLength = fragmentLength();
        setupMessage(srcLength);
        onFragment(srcLength, CONTINUE, firstHandler);

        // Second replay
        bufferContainsExampleMessage(true, SESSION_ID_2, SEQUENCE_NUMBER, SEQUENCE_INDEX);
        setupMessage(srcLength);
        onFragment(srcLength, CONTINUE, secondHandler);

        assertHasResentWithPossDupFlag(srcLength, times(2));
    }

    // TODO: queue replay requests by fix session

    @Test
    public void shouldGapFillAdminMessages()
    {
        final int offset = setupCapturingClaim();
        onReplay(END_SEQ_NO, inv ->
        {
            onTestRequest(SEQUENCE_NUMBER);

            return true;
        });

        replayer.doWork();

        assertSentGapFill(SEQUENCE_NUMBER, END_SEQ_NO + 1, offset, times(1));

        replayer.doWork();

        verifyReplayCompleteMessageSent();
    }

    @Test
    public void shouldGapFillOnceForTwoConsecutiveAdminMessages()
    {
        final int endSeqNo = endSeqNoForTwoMessages();
        final int offset = setupCapturingClaim();
        setReplayedMessages(2);

        onReplay(endSeqNo, inv ->
        {
            onTestRequest(SEQUENCE_NUMBER);

            onTestRequest(SEQUENCE_NUMBER + 1);

            return true;
        });

        replayer.doWork();

        assertSentGapFill(SEQUENCE_NUMBER, endSeqNo + 1, offset, times(1));

        replayer.doWork();

        verifyReplayCompleteMessageSent();
    }

    @Test
    public void shouldGapFillOnceForTwoConsecutiveAdminMessagesWhenBackPressured()
    {
        final int endSeqNo = endSeqNoForTwoMessages();

        backpressureTryClaim();

        setReplayedMessages(2);
        whenReplayQueried().then(inv ->
        {
            onTestRequest(SEQUENCE_NUMBER);

            onTestRequest(SEQUENCE_NUMBER + 1);

            return true;
        });

        // try to send the gap fill, but get back pressured
        final long result = bufferHasResendRequest(endSeqNo);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);
        replayer.doWork(); // receives the callback with the two test requests

        replayer.doWork(); // attempts the replay and get's back pressured
        claimedAndNothingMore();

        // resend the gap fill, don't get back pressured
        final int offset = setupCapturingClaim();
        replayer.doWork();
        assertSentGapFill(SEQUENCE_NUMBER, endSeqNo + 1, offset, times(1));

        replayer.doWork();

        verifyReplayCompleteMessageSent();
    }

    @Test
    public void shouldGapFillForAdminMessagesFollowedByAppMessage()
    {
        final int endSeqNo = endSeqNoForTwoMessages();
        final int offset = setupCapturingClaim();

        setReplayedMessages(2);

        onReplay(endSeqNo, inv ->
        {
            onTestRequest(BEGIN_SEQ_NO);

            final int srcLength = onExampleMessage(endSeqNo);

            assertResentGapFillThenMessage(endSeqNo, offset, srcLength);

            return true;
        });
    }

    /**
     * Replays two example messages, sequence number of BEGIN_SEQ_NO and BEGIN_SEQ_NO + 1
     */
    @Test
    public void shouldResendTwoAppMessages()
    {
        final int endSeqNo = endSeqNoForTwoMessages();

        setReplayedMessages(2);

        onReplay(endSeqNo, inv ->
        {
            setupCapturingClaim();

            final int srcLength = onExampleMessage(BEGIN_SEQ_NO);

            onExampleMessage(endSeqNo);

            assertHasResentWithPossDupFlag(srcLength, times(2));

            return true;
        });

        replayer.doWork();

        assertReplayHandlerInvoked(endSeqNo);

        replayer.doWork();

        verifyReplayCompleteMessageSent();
    }

    private void assertReplayHandlerInvoked(final int endSeqNo)
    {
        final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);

        verify(replayHandler, times(2)).onReplayedMessage(
            bufferCaptor.capture(),
            offsetCaptor.capture(),
            lengthCaptor.capture(),
            eq(LIBRARY_ID),
            eq(SESSION_ID),
            eq(SEQUENCE_INDEX),
            eq(MESSAGE_TYPE));

        final HeaderDecoder fixHeader = new HeaderDecoder();
        final AsciiBuffer asciiBuffer = new MutableAsciiBuffer(bufferCaptor.getValue());
        fixHeader.decode(asciiBuffer, offsetCaptor.getValue(), lengthCaptor.getValue());

        assertEquals(Constants.VERSION, fixHeader.beginStringAsString());
        assertEquals(BUFFER_SENDER, fixHeader.senderCompIDAsString());
        assertEquals(BUFFER_TARGET, fixHeader.targetCompIDAsString());
        assertEquals(ExampleMessageDecoder.MESSAGE_TYPE_AS_STRING, fixHeader.msgTypeAsString());
        assertEquals(endSeqNo, fixHeader.msgSeqNum());
    }

    @Test
    public void shouldResendTwoAppMessagesWhenBackPressured()
    {
        final int endSeqNo = endSeqNoForTwoMessages();

        // First message resent
        whenReplayQueried().then(inv ->
        {
            setupCapturingClaim();

            // First message resent
            final int srcLength1 = onExampleMessage(BEGIN_SEQ_NO);
            assertHasResentWithPossDupFlag(srcLength1, times(1));

            return false;
        });

        final long result = bufferHasResendRequest(endSeqNo);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);

        replayer.doWork();

        verifyClaim();
        resetMocks();

        // Second message back pressured
        backpressureTryClaim();
        onExampleMessage(endSeqNo, ABORT);

        // Resend of second message.
        replayer.doWork();

        setupCapturingClaim();
        final int srcLength = onExampleMessage(endSeqNo);
        assertHasResentWithPossDupFlag(srcLength, times(1));
    }

    private void resetMocks()
    {
        reset(publication, claim, replayQuery, replayOperation);
    }

    @Test
    public void shouldResendAppThenAdminGapFillWhenBackPressured()
    {
        final int endSeqNo = endSeqNoForTwoMessages();
        setReplayedMessages(2);

        whenReplayQueried().then(inv ->
        {
            setupCapturingClaim();
            final int srcLength = onExampleMessage(BEGIN_SEQ_NO);
            assertHasResentWithPossDupFlag(srcLength, times(1));

            backpressureTryClaim();
            onTestRequest(endSeqNo);

            return true;
        });

        // Processes the resend request
        final long result = bufferHasResendRequest(endSeqNo);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);

        // Processes the backpressured try claim
        replayer.doWork();

        verifyClaim();
        resetMocks();
        whenReplayQueried().thenReturn(true);

        final int offset = setupCapturingClaim();

        replayer.doWork();

        assertSentGapFill(endSeqNo, endSeqNo + 1, offset, times(1));

        replayer.doWork();

        verifyReplayCompleteMessageSent();
    }

    @Test
    public void shouldGapFillMissingMesages()
    {
        final int endSeqNo = endSeqNoForTwoMessages();

        final int offset = setupCapturingClaim();
        setReplayedMessages(0);

        final long result = bufferHasResendRequest(endSeqNo);
        onRequestResendMessage(result);

        replayer.doWork();

        assertSentGapFill(SEQUENCE_NUMBER, endSeqNo + 1, offset, times(1));
        verifyIllegalStateException();
    }

    @Test
    public void shouldGapFillMissingMesagesWhenBackPressured()
    {
        final int endSeqNo = endSeqNoForTwoMessages();

        backpressureTryClaim();
        setReplayedMessages(0);

        final long result = bufferHasResendRequest(endSeqNo);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);

        replayer.doWork();

        claimedAndNothingMore();

        final int offset = setupCapturingClaim();
        setReplayedMessages(0);

        replayer.doWork();

        assertSentGapFill(SEQUENCE_NUMBER, endSeqNo + 1, offset, times(1));
        verifyIllegalStateException();
    }

    @Test
    public void shouldGapFillMissingMesagesFollowedByApplicationMessage()
    {
        final int endSeqNo = endSeqNoForTwoMessages();

        onReplay(endSeqNo, inv ->
        {
            final int offset = setupCapturingClaim();

            final int srcLength = onExampleMessage(endSeqNo);

            assertResentGapFillThenMessage(BEGIN_SEQ_NO, offset, srcLength);

            return true;
        });

        replayer.doWork();

        verifyIllegalStateException();
    }

    @Test
    public void shouldReplayMessageWithExpandingBodyLength()
    {
        onReplay(END_SEQ_NO, inv ->
        {
            bufferContainsMessage(MESSAGE_REQUIRING_LONGER_BODY_LENGTH);

            final int srcLength = fragmentLength();
            setupCapturingClaim();

            onFragment(srcLength);

            assertHasResentWithPossDupFlag(claimedLength, times(1));
            hasNotOverwrittenSeperatorChar();

            assertEndsWithValidChecksum(offset + 1);

            return true;
        });
    }

    @Test
    public void shouldReplayMessageWithExpandingBodyLengthWhenBackPressured()
    {
        whenReplayQueried().then(inv ->
        {
            bufferContainsMessage(MESSAGE_REQUIRING_LONGER_BODY_LENGTH);

            backpressureTryClaim();

            onFragment(fragmentLength(), COMMIT, getHandler());

            verifyClaim();

            return true;
        });

        final long result = bufferHasResendRequest(END_SEQ_NO);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);

        verifyPublicationOnlyPayloadQueried();
        verifyNoMoreInteractions(claim);
        resetMocks();

        shouldReplayMessageWithExpandingBodyLength();
    }

    @Test
    public void shouldPublishMessagesWithoutSetPossDupFlag()
    {
        onReplay(END_SEQ_NO, inv ->
        {
            bufferContainsExampleMessage(false);
            final int srcLength = fragmentLength();
            setupCapturingClaim();

            onFragment(srcLength);

            assertHasResentWithPossDupFlag(claimedLength, times(1));

            final int afterOffset = this.offset + 1;
            assertThat(resultAsciiBuffer,
                sequenceEqualsAscii("8=FIX.4.4\0019=86\001", afterOffset));

            assertThat(resultAsciiBuffer,
                sequenceEqualsAscii("8=FIX.4.4\0019=86\001", afterOffset));

            assertEndsWithValidChecksum(afterOffset);

            return true;
        });
    }

    @Test
    public void shouldIgnoreIrrelevantFixMessages()
    {
        onMessage(LogonDecoder.MESSAGE_TYPE, buffer.capacity(), CONTINUE);

        verifyNoMoreInteractions(replayQuery, publication);
    }

    @Test
    public void shouldValidateResendRequestMessageSequenceNumbers()
    {
        final long result = bufferHasResendRequest(BEGIN_SEQ_NO - 1);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, CONTINUE);

        verify(errorHandler).onError(any());
        verifyNoMoreInteractions(replayQuery, publication);
    }

    @After
    public void shouldHaveNoMoreErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    private void claimedAndNothingMore()
    {
        verifyClaim();
        verifyPublicationOnlyPayloadQueried();
        verifyNoMoreInteractions(claim);
        reset(publication);
    }

    private void assertResentGapFillThenMessage(final int endSeqNo, final int offset, final int srcLength)
    {
        doAnswer(commitInv ->
        {
            assertSentGapFill(SEQUENCE_NUMBER, endSeqNo, offset, times(2));
            return null;
        }).when(claim).commit();

        assertHasResentWithPossDupFlag(srcLength, times(2));
    }

    private int onExampleMessage(final int endSeqNo)
    {
        return onExampleMessage(endSeqNo, CONTINUE);
    }

    private int onExampleMessage(final int endSeqNo, final Action expectedAction)
    {
        bufferContainsExampleMessage(true, SESSION_ID, endSeqNo, SEQUENCE_INDEX);
        final int srcLength = fragmentLength();
        onFragment(srcLength, expectedAction, getHandler());
        return srcLength;
    }

    private void onTestRequest(final int sequenceNumber)
    {
        bufferContainsTestRequest(sequenceNumber);
        onFragment(fragmentLength(), CONTINUE, getHandler());
    }

    private int endSeqNoForTwoMessages()
    {
        // inclusive numbering
        return BEGIN_SEQ_NO + 1;
    }

    private void onFragment(final int length)
    {
        onFragment(length, CONTINUE, getHandler());
    }

    private void onReplay(final int endSeqNo, final Answer<Boolean> answer)
    {
        whenReplayQueried().then(answer);

        final long result = bufferHasResendRequest(endSeqNo);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);
    }

    private void onReplayOtherSession(final int endSeqNo)
    {
        whenReplayQueried().thenReturn(true);

        final long result = bufferHasResendRequest(endSeqNo, RESEND_TARGET_2);
        onMessageWithSession(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT, SESSION_ID_2, CONNECTION_ID_2);
    }

    private void onFragment(final int length, final Action expectedAction, final ControlledFragmentHandler handler)
    {
        final Action action = handler.onFragment(buffer, START, length, fragmentHeader);
        assertEquals(expectedAction, action);
    }

    private ControlledFragmentHandler getHandler()
    {
        return handler.getValue();
    }

    private void verifyIllegalStateException()
    {
        verify(errorHandler).onError(any(IllegalStateException.class));
    }

    private void assertHasResentWithPossDupFlag(final int srcLength, final VerificationMode times)
    {
        verify(publication, atLeastOnce()).tryClaim(srcLength, claim);
        assertResultBufferHasSetPossDupFlagAndSendingTimeUpdates();
        verifyCommit(times);
    }

    private void assertSentGapFill(
        final int msgSeqNum,
        final int newSeqNo,
        final int offset,
        final VerificationMode times)
    {
        verifyClaim();
        assertResultBufferHasGapFillMessage(resultBuffer.capacity() - offset, msgSeqNum, newSeqNo);
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
        assertEquals("9=126\00135=1\001", lengthSection);
    }

    private void assertResultBufferHasGapFillMessage(
        final int claimedLength,
        final int msgSeqNum,
        final int newSeqNo)
    {
        final int offset = offset() + MESSAGE_FRAME_BLOCK_LENGTH;
        final int length = claimedLength - MESSAGE_FRAME_BLOCK_LENGTH;
        final String message = resultAsciiBuffer.getAscii(offset, length);
        final SequenceResetDecoder sequenceReset = new SequenceResetDecoder();
        sequenceReset.decode(resultAsciiBuffer, offset, length);
        final HeaderDecoder header = sequenceReset.header();

        if (!sequenceReset.validate())
        {
            fail(message + "%n" + sequenceReset.invalidTagId() + " " +
                RejectReason.decode(sequenceReset.rejectReason()));
        }

        assertTrue(message, sequenceReset.gapFillFlag());
        assertEquals(message, msgSeqNum, header.msgSeqNum());
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
        verify(replayQuery).query(
            any(),
            eq(SESSION_ID),
            eq(BEGIN_SEQ_NO),
            eq(SEQUENCE_INDEX),
            eq(endSeqNo),
            eq(SEQUENCE_INDEX),
            any());
    }

    private void assertResultBufferHasSetPossDupFlagAndSendingTimeUpdates()
    {
        final String resultAsAscii = resultAsciiBuffer.getAscii(0, resultAsciiBuffer.capacity());
        assertThat(resultAsAscii, containsString("43=Y"));

        assertThat(resultAsAscii,
            containsString(ORIG_SENDING_TIME_PREFIX_AS_STR + ORIGINAL_SENDING_TIME + '\001'));

        assertThat(resultAsAscii,
            containsString("52=" + DATE_TIME_STR + '\001'));
    }

    private void onRequestResendMessage(final long result)
    {
        onMessage(ResendRequestDecoder.MESSAGE_TYPE, result, COMMIT);
    }

    private void onMessage(final int messageType, final long result, final Action expectedAction)
    {
        onMessageWithSession(messageType, result, expectedAction, SESSION_ID, CONNECTION_ID);
    }

    private void onMessageWithSession(
        final int messageType,
        final long result,
        final Action expectedAction,
        final long sessionId,
        final long connectionId)
    {
        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);
        final Action action = replayer.onMessage(
            buffer, offset, length,
            LIBRARY_ID, connectionId, sessionId, SEQUENCE_INDEX, messageType, 0L, OK, 0, 0L);
        assertEquals(expectedAction, action);
    }

    private void bufferContainsMessage(final byte[] message)
    {
        logEntryLength = message.length;
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(message);
        bufferContainsMessage(SESSION_ID, SEQUENCE_NUMBER, asciiBuffer, MESSAGE_TYPE);
    }

    private void verifyPublicationOnlyPayloadQueried()
    {
        verify(publication).maxPayloadLength();
        verifyNoMoreInteractions(publication);
    }

    private void verifyReplayCompleteMessageSent()
    {
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
        final ReplayCompleteDecoder replayComplete = new ReplayCompleteDecoder();

        int offset = offset();
        messageHeader.wrap(resultBuffer, offset);
        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        assertEquals(ReplayCompleteDecoder.TEMPLATE_ID, messageHeader.templateId());
        replayComplete.wrap(resultBuffer, offset, messageHeader.blockLength(), messageHeader.version());
        assertEquals(CONNECTION_ID, replayComplete.connection());
    }
}
