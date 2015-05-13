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
package uk.co.real_logic.fix_gateway.logger;

import org.junit.Test;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.decoder.ResendRequestDecoder;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.logger.Replayer.POSS_DUP_FIELD;
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

public class ReplayerTest extends AbstractMessageTest
{

    private static final int BEGIN_SEQ_NO = 2;
    private static final int END_SEQ_NO = 2;

    private ReplayQuery mockReplayQuery = mock(ReplayQuery.class);
    private Publication mockPublication = mock(Publication.class);
    private GatewaySubscription mockSubscription = mock(GatewaySubscription.class);
    private BufferClaim mockClaim = mock(BufferClaim.class);
    private IdleStrategy idleStrategy = mock(IdleStrategy.class);

    private Replayer replayer = new Replayer(mockSubscription, mockReplayQuery, mockPublication, mockClaim, idleStrategy);

    private UnsafeBuffer resultBuffer = new UnsafeBuffer(new byte[16 * 1024]);

    @Test
    public void shouldParseResendRequest()
    {
        bufferHasResendRequest(END_SEQ_NO);

        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyQueriedService();
        verifyNoMoreInteractions(mockPublication);
    }

    @Test
    public void shouldPublishMessagesWithSetPossDupFlag()
    {
        bufferContainsMessage(true);
        final int srcLength = messageLength();
        setupClaim(srcLength);
        setupPublication(srcLength);

        replayer.onLogEntry(null, buffer, START, offset, srcLength);

        verifyClaim(srcLength);
        assertHasSetPossDupFlag();
        verifyCommit();
    }

    @Test
    public void shouldPublishMessagesWithoutSetPossDupFlag()
    {
        bufferContainsMessage(false);
        final int srcLength = messageLength();
        setupClaim(srcLength);
        setupPublication(srcLength);

        replayer.onLogEntry(null, buffer, START, offset, srcLength);

        verifyClaim(srcLength + POSS_DUP_FIELD.length);
        assertHasSetPossDupFlag();
        verifyCommit();
    }

    @Test
    public void shouldIgnoreIrrelevantFixMessages()
    {
        onMessage(LogonDecoder.MESSAGE_TYPE);

        verifyNoMoreInteractions(mockReplayQuery, mockPublication);
    }

    @Test
    public void shouldValidateResendRequestMessageSequenceNumbers()
    {
        bufferHasResendRequest(BEGIN_SEQ_NO - 1);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyNoMoreInteractions(mockReplayQuery, mockPublication);
    }

    private void setupPublication(final int srcLength)
    {
        when(mockPublication.tryClaim(srcLength, mockClaim)).thenReturn((long) srcLength);
    }

    private void setupClaim(final int srcLength)
    {
        when(mockClaim.buffer()).thenReturn(resultBuffer);
        when(mockClaim.offset()).thenReturn(START + 1);
        when(mockClaim.length()).thenReturn(srcLength);
    }

    private void verifyClaim(final int srcLength)
    {
        verify(mockPublication).tryClaim(srcLength, mockClaim);
    }

    private void verifyCommit()
    {
        verify(mockClaim).commit();
    }

    private void verifyQueriedService()
    {
        verify(mockReplayQuery).query(replayer, SESSION_ID, BEGIN_SEQ_NO, END_SEQ_NO);
    }

    private void assertHasSetPossDupFlag()
    {
        final int possDupIndex = new AsciiFlyweight(resultBuffer).scan(0, resultBuffer.capacity(), 'Y');
        assertNotEquals("Unable to find poss dup index", UNKNOWN_INDEX, possDupIndex);
    }

    private void bufferHasResendRequest(final int endSeqNo)
    {
        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();

        resendRequest
            .beginSeqNo(BEGIN_SEQ_NO)
            .endSeqNo(endSeqNo)
            .encode(new MutableAsciiFlyweight(buffer), 1);
    }

    private void onMessage(final int messageType)
    {
        replayer.onMessage(buffer, 1, buffer.capacity(), CONNECTION_ID, SESSION_ID, messageType);
    }
}
