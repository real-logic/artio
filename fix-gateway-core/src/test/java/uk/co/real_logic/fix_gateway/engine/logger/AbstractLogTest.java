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
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MESSAGE_FRAME_BLOCK_LENGTH;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.SIZE_OF_LENGTH_FIELD;

public class AbstractLogTest
{
    protected static final long SESSION_ID = 1;
    protected static final long SESSION_ID_2 = 2;
    protected static final long CONNECTION_ID = 1;
    protected static final int STREAM_ID = OUTBOUND_LIBRARY_STREAM;
    protected static final int START = 1;
    protected static final int SEQUENCE_NUMBER = 5;
    protected static final int AERON_SESSION_ID = -10;
    protected static final int LIBRARY_ID = 7;
    protected static final int BEGIN_SEQ_NO = 2;
    protected static final int END_SEQ_NO = 2;

    protected MessageHeaderEncoder header = new MessageHeaderEncoder();
    protected FixMessageEncoder messageFrame = new FixMessageEncoder();

    protected Publication publication = mock(Publication.class);
    protected BufferClaim claim = mock(BufferClaim.class);
    protected UnsafeBuffer resultBuffer = new UnsafeBuffer(new byte[16 * 1024]);

    protected UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);

    protected int logEntryLength;
    protected int offset;

    protected void bufferContainsMessage(final boolean hasPossDupFlag)
    {
        bufferContainsMessage(hasPossDupFlag, SESSION_ID, SEQUENCE_NUMBER);
    }

    protected void bufferContainsMessage(final boolean hasPossDupFlag, final long sessionId, int sequenceNumber)
    {
        final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
        timestampEncoder.encode(System.currentTimeMillis());
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(new byte[450]);
        final TestRequestEncoder testRequest = new TestRequestEncoder();

        testRequest
            .testReqID("abc")
            .header()
                .sendingTime(timestampEncoder.buffer())
                .senderCompID("sender")
                .targetCompID("target")
                .msgSeqNum(sequenceNumber);

        if (hasPossDupFlag)
        {
            testRequest
                .header()
                .possDupFlag(false);
        }

        logEntryLength = testRequest.encode(asciiBuffer, 0);

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
            .messageType(TestRequestDecoder.MESSAGE_TYPE)
            .session(sessionId)
            .connection(CONNECTION_ID)
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

    protected void bufferHasResendRequest(final int endSeqNo)
    {
        final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
        timestampEncoder.encode(System.currentTimeMillis());

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();

        resendRequest
            .header()
            .sendingTime(timestampEncoder.buffer())
            .msgSeqNum(1)
            .senderCompID("sender")
            .targetCompID("target");

        resendRequest
            .beginSeqNo(BEGIN_SEQ_NO)
            .endSeqNo(endSeqNo)
            .encode(new MutableAsciiBuffer(buffer), 1);
    }

    protected void setupPublication(final int srcLength)
    {
        when(publication.tryClaim(srcLength, claim)).thenReturn((long)srcLength);
    }

    protected void setupClaim(final int srcLength)
    {
        when(claim.buffer()).thenReturn(resultBuffer);
        when(claim.offset()).thenReturn(START + 1);
        when(claim.length()).thenReturn(srcLength);
    }

    protected void verifyClaim(final int srcLength)
    {
        verify(publication).tryClaim(srcLength - MESSAGE_FRAME_BLOCK_LENGTH, claim);
    }

    protected void verifyCommit()
    {
        verify(claim).commit();
    }
}
