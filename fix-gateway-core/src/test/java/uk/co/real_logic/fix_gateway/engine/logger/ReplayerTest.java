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

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MOST_RECENT_MESSAGE;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.util.AsciiBuffer.UNKNOWN_INDEX;

public class ReplayerTest extends AbstractLogTest
{
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
        bufferHasResendRequest(END_SEQ_NO);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyQueriedService(END_SEQ_NO);
        verifyNoMoreInteractions(publication);
    }

    @Test
    public void shouldPublishAllRemainingMessages()
    {
        bufferHasResendRequest(MOST_RECENT_MESSAGE);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

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
    public void shouldPublishMessagesWithoutSetPossDupFlag()
    {
        bufferContainsMessage(false);
        final int srcLength = setupMessage();

        replayer.onFragment(buffer, START, SIZE_OF_FRAME + srcLength, null);

        verifyClaim(SIZE_OF_FRAME + srcLength + PossDupEnabler.POSS_DUP_FIELD.length);
        assertHasSetPossDupFlag();
        verifyCommit();
    }

    @Test
    public void shouldIgnoreIrrelevantFixMessages()
    {
        onMessage(LogonDecoder.MESSAGE_TYPE);

        verifyNoMoreInteractions(replayQuery, publication);
    }

    @Test
    public void shouldValidateResendRequestMessageSequenceNumbers()
    {
        bufferHasResendRequest(BEGIN_SEQ_NO - 1);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

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
        final int possDupIndex = new MutableAsciiBuffer(resultBuffer).scan(0, resultBuffer.capacity(), 'Y');
        assertNotEquals("Unable to find poss dup index", UNKNOWN_INDEX, possDupIndex);
    }

    private void onMessage(final int messageType)
    {
        replayer.onMessage(
            buffer, 1, buffer.capacity(), LIBRARY_ID, CONNECTION_ID, SESSION_ID, SEQUENCE_INDEX, messageType, 0L, OK, 0L);
    }
}
