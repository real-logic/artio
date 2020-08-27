/*
 * Copyright 2015-2020 Real Logic Limited., Monotonic Ltd.
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
import org.agrona.DirectBuffer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.decoder.SequenceResetDecoder;
import uk.co.real_logic.artio.engine.ReplayerCommandQueue;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;

public class GapFillerTest extends AbstractLogTest
{
    private final GatewayPublication publication = mock(GatewayPublication.class);
    private final Subscription subscription = mock(Subscription.class);
    private final SenderSequenceNumbers senderSequenceNumbers = mock(SenderSequenceNumbers.class);
    private final GapFiller gapFiller = new GapFiller(
        subscription, publication, DEFAULT_NAME_PREFIX, senderSequenceNumbers,
        mock(ReplayerCommandQueue.class), new FakeFixSessionCodecsFactory());

    @Test
    public void shouldGapFillInResponseToResendRequest()
    {
        final long result = bufferHasResendRequest(END_SEQ_NO);
        final int encodedLength = Encoder.length(result);
        final int encodedOffset = Encoder.offset(result);
        gapFiller.onMessage(
            buffer, encodedOffset, encodedLength,
            LIBRARY_ID, CONNECTION_ID, SESSION_ID, SEQUENCE_INDEX,
            ResendRequestDecoder.MESSAGE_TYPE, 0L, OK, 0, 0L, 0);

        final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        final ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(int.class);
        final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(int.class);

        verify(publication).saveMessage(
            bufferCaptor.capture(), offsetCaptor.capture(), lengthCaptor.capture(),
            eq(LIBRARY_ID), eq(SequenceResetDecoder.MESSAGE_TYPE),
            eq(SESSION_ID), eq(SEQUENCE_INDEX), eq(CONNECTION_ID),
            eq(OK), eq(END_SEQ_NO));

        final AsciiBuffer buffer = new MutableAsciiBuffer(bufferCaptor.getValue());
        final int offset = offsetCaptor.getValue();
        final int length = lengthCaptor.getValue();
        final SequenceResetDecoder sequenceReset = new SequenceResetDecoder();
        sequenceReset.decode(buffer, offset, length);

        assertTrue(sequenceReset.hasGapFillFlag());
        assertTrue(sequenceReset.gapFillFlag());
        assertEquals(END_SEQ_NO, sequenceReset.newSeqNo());
    }
}
