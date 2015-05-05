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

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.decoder.ResendRequestDecoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.logger.Replayer.SIZE_OF_LENGTH_FIELD;

public class ReplayerTest
{
    private static final long SESSION_ID = 1;
    private static final int BEGIN_SEQ_NO = 2;
    private static final int END_SEQ_NO = 2;
    private static final int CONNECTION_ID = 1;
    private static final int START_OFFSET = 1;

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final FixMessageEncoder messageFrame = new FixMessageEncoder();

    private QueryService mockQueryService = mock(QueryService.class);
    private Publication mockPublication = mock(Publication.class);
    private BufferClaim mockBufferClaim = mock(BufferClaim.class);

    private Replayer replayer = new Replayer(mockQueryService, mockPublication);
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[16 * 1024]);
    private UnsafeBuffer replayBuffer = new UnsafeBuffer(new byte[16 * 1024]);

    private int possDupPositionOffset;

    @Test
    public void shouldParseResendRequest()
    {
        bufferHasResendRequest(END_SEQ_NO);
        queryServiceHasMessage();
        bufferClaimRefersToMessage(true);

        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyQueriedService();
        verify(mockBufferClaim).commit();
        assertHasSetPossDupFlag();
    }

    @Ignore
    @Test
    public void shouldParseResendRequestWithMissingPossDupFlag()
    {
        bufferHasResendRequest(END_SEQ_NO);
        queryServiceHasMessage();
        bufferClaimRefersToMessage(false);

        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyQueriedService();
        verify(mockBufferClaim).commit();
        assertHasSetPossDupFlag();
    }

    @Test
    public void shouldIgnoreIrrelevantFixMessages()
    {
        onMessage(LogonDecoder.MESSAGE_TYPE);

        verifyNoMoreInteractions(mockQueryService, mockPublication);
    }

    @Test
    public void shouldValidateResendRequestMessageSequenceNumbers()
    {
        bufferHasResendRequest(BEGIN_SEQ_NO - 1);
        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyNoMoreInteractions(mockQueryService, mockPublication);
    }

    @Test
    public void shouldHandleMissingMessages()
    {
        bufferHasResendRequest(END_SEQ_NO);
        queryServiceDoesNotHaveMessage();

        onMessage(ResendRequestDecoder.MESSAGE_TYPE);

        verifyQueriedService();
        verifyNoMoreInteractions(mockBufferClaim);
    }

    private void verifyQueriedService()
    {
        verify(mockQueryService).query(mockPublication, SESSION_ID, BEGIN_SEQ_NO, END_SEQ_NO);
    }

    private void assertHasSetPossDupFlag()
    {
        assertEquals((byte) 'Y', replayBuffer.getByte(possDupPositionOffset));
    }

    private void queryServiceHasMessage()
    {
        whenQueryService().thenReturn(mockBufferClaim);
    }

    private void queryServiceDoesNotHaveMessage()
    {
        whenQueryService().thenReturn(null);
    }

    private OngoingStubbing<BufferClaim> whenQueryService()
    {
        return when(mockQueryService.query(eq(mockPublication), anyInt(), anyInt(), anyInt()));
    }

    private void bufferClaimRefersToMessage(final boolean hasPossDupFlag)
    {
        final UnsafeBuffer msgBuffer = new UnsafeBuffer(new byte[8 * 1024]);
        final MutableAsciiFlyweight asciiFlyweight = new MutableAsciiFlyweight(msgBuffer);
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("abc");
        if (hasPossDupFlag)
        {
            testRequest.header().possDupFlag(false);
        }
        final int length = testRequest.encode(asciiFlyweight, 0);
        System.out.println(asciiFlyweight.getAscii(0, length));

        int index = START_OFFSET;
        header
            .wrap(replayBuffer, index, 0)
            .blockLength(messageFrame.sbeBlockLength())
            .templateId(messageFrame.sbeTemplateId())
            .schemaId(messageFrame.sbeSchemaId())
            .version(messageFrame.sbeSchemaVersion());

        index += header.size();

        messageFrame
            .wrap(replayBuffer, index)
            .messageType(TestRequestDecoder.MESSAGE_TYPE)
            .session(SESSION_ID)
            .connection(CONNECTION_ID)
            .putBody(msgBuffer, 0, length);

        index += messageFrame.sbeBlockLength() + SIZE_OF_LENGTH_FIELD;
        possDupPositionOffset = index + asciiFlyweight.scan(0, length, 'N');

        when(mockBufferClaim.buffer()).thenReturn(replayBuffer);
        when(mockBufferClaim.length()).thenReturn(messageFrame.limit() - START_OFFSET);
        when(mockBufferClaim.offset()).thenReturn(START_OFFSET);
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
