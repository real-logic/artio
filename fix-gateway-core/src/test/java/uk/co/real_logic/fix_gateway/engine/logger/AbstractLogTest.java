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

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.decoder.ExampleMessageDecoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.SIZE_OF_LENGTH_FIELD;

public class AbstractLogTest
{
    protected static final long SESSION_ID = 1;
    protected static final long SESSION_ID_2 = 2;
    protected static final long CONNECTION_ID = 1;
    protected static final int STREAM_ID = OUTBOUND_LIBRARY_STREAM;
    protected static final int START = FRAME_ALIGNMENT;
    protected static final int SEQUENCE_NUMBER = 2;
    protected static final int AERON_SESSION_ID = -10;
    protected static final int LIBRARY_ID = 7;
    protected static final int BEGIN_SEQ_NO = 2;
    protected static final int END_SEQ_NO = 2;
    protected static final int SEQUENCE_INDEX = 1;
    protected static final int ENCODE_OFFSET = 1;
    public static final String BUFFER_SENDER = "sender";
    public static final String BUFFER_TARGET = "target";
    public static final String RESEND_SENDER = "target";
    public static final String RESEND_TARGET = "sender";

    protected MessageHeaderEncoder header = new MessageHeaderEncoder();
    protected FixMessageEncoder messageFrame = new FixMessageEncoder();

    protected Publication publication = mock(Publication.class);
    protected BufferClaim claim = mock(BufferClaim.class);
    protected UnsafeBuffer resultBuffer;
    protected MutableAsciiBuffer resultAsciiBuffer = new MutableAsciiBuffer();

    protected UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);

    protected int logEntryLength;
    protected int offset;

    protected void bufferContainsExampleMessage(final boolean hasPossDupFlag)
    {
        bufferContainsExampleMessage(hasPossDupFlag, SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_INDEX);
    }

    protected void bufferContainsExampleMessage(
        final boolean hasPossDupFlag, final long sessionId, final int sequenceNumber, final int sequenceIndex)
    {
        final ExampleMessageEncoder exampleMessage = new ExampleMessageEncoder();
        final HeaderEncoder header = exampleMessage.header();
        exampleMessage.testReqID("abc");

        if (hasPossDupFlag)
        {
            // NB: set to false to check that it gets flipped upon resend
            header.possDupFlag(false);
        }

        bufferContainsMessage(
            sessionId, sequenceNumber, sequenceIndex, exampleMessage, header, ExampleMessageDecoder.MESSAGE_TYPE);
    }

    protected void bufferContainsTestRequest(final int sequenceNumber)
    {
        final TestRequestEncoder testRequestEncoder = new TestRequestEncoder();
        final HeaderEncoder header = testRequestEncoder.header();
        testRequestEncoder.testReqID("abc");
        header.possDupFlag(false);

        bufferContainsMessage(
            SESSION_ID, sequenceNumber, SEQUENCE_INDEX, testRequestEncoder, header, TestRequestDecoder.MESSAGE_TYPE);
    }

    private void bufferContainsMessage(
        final long sessionId,
        final int sequenceNumber,
        final int sequenceIndex,
        final Encoder exampleMessage,
        final HeaderEncoder header,
        final int messageType)
    {
        final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
        timestampEncoder.encode(System.currentTimeMillis());
        MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(new byte[450]);

        header
            .sendingTime(timestampEncoder.buffer())
            .senderCompID(BUFFER_SENDER)
            .targetCompID(BUFFER_TARGET)
            .msgSeqNum(sequenceNumber);

        final long result = exampleMessage.encode(asciiBuffer, 0);
        logEntryLength = Encoder.length(result);
        final int encodedOffset = Encoder.offset(result);
        asciiBuffer = new MutableAsciiBuffer(asciiBuffer, encodedOffset, logEntryLength);

        bufferContainsMessage(sessionId, sequenceIndex, asciiBuffer, messageType);
    }

    protected void bufferContainsMessage(
        final long sessionId,
        final int sequenceIndex,
        final MutableAsciiBuffer asciiBuffer,
        final int messageType)
    {
        offset = START;
        header
            .wrap(buffer, offset)
            .blockLength(messageFrame.sbeBlockLength())
            .templateId(messageFrame.sbeTemplateId())
            .schemaId(messageFrame.sbeSchemaId())
            .version(messageFrame.sbeSchemaVersion());

        offset += header.encodedLength();

        messageFrame
            .wrap(buffer, offset)
            .messageType(messageType)
            .session(sessionId)
            .connection(CONNECTION_ID)
            .sequenceIndex(sequenceIndex)
            .putBody(asciiBuffer, 0, logEntryLength);

        offset += messageFrame.sbeBlockLength() + SIZE_OF_LENGTH_FIELD;
    }

    protected int fragmentLength()
    {
        return endPosition() - START;
    }

    protected int endPosition()
    {
        return offset + logEntryLength;
    }

    protected int alignedEndPosition()
    {
        return BitUtil.align(endPosition(), FRAME_ALIGNMENT);
    }

    protected long bufferHasResendRequest(final int endSeqNo)
    {
        final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
        timestampEncoder.encode(System.currentTimeMillis());

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();

        resendRequest
            .header()
            .sendingTime(timestampEncoder.buffer())
            .msgSeqNum(1)
            .senderCompID(RESEND_SENDER)
            .targetCompID(RESEND_TARGET);

        return resendRequest
            .beginSeqNo(BEGIN_SEQ_NO)
            .endSeqNo(endSeqNo)
            .encode(new MutableAsciiBuffer(buffer), ENCODE_OFFSET);
    }

    protected void setupPublication(final int srcLength)
    {
        when(publication.tryClaim(srcLength, claim)).thenReturn((long)srcLength);
    }

    protected void backpressureTryClaim()
    {
        when(publication.tryClaim(anyInt(), any())).thenReturn(Publication.BACK_PRESSURED);
    }

    protected void setupClaim(final int srcLength)
    {
        final int offset = offset();
        resultBuffer = new UnsafeBuffer(new byte[offset + srcLength]);
        resultAsciiBuffer.wrap(resultBuffer);
        when(claim.buffer()).thenReturn(resultBuffer);
        when(claim.offset()).thenReturn(offset);
        when(claim.length()).thenReturn(srcLength);
    }

    protected int setupCapturingClaim()
    {
        final int offset = offset();
        when(publication.tryClaim(anyInt(), eq(claim))).then(inv ->
        {
            final int length = (int) inv.getArguments()[0];
            resultBuffer = new UnsafeBuffer(new byte[offset + length]);
            resultAsciiBuffer.wrap(resultBuffer);
            when(claim.buffer()).thenReturn(resultBuffer);
            when(claim.offset()).thenReturn(offset);
            when(claim.length()).thenReturn(length);

            return (long) offset;
        });
        return offset;
    }

    protected int offset()
    {
        return START + 1;
    }

    protected void verifyClaim(final int srcLength)
    {
        verify(publication).tryClaim(srcLength, claim);
    }

    protected void verifyCommit(final VerificationMode times)
    {
        verify(claim, times).commit();
    }
}
