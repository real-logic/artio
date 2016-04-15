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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.protocol.ClaimablePublication;

class CatchupReplayer implements FragmentHandler
{
    private static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixMessageEncoder.BLOCK_LENGTH + FixMessageEncoder.bodyHeaderLength();
    private final FixMessageEncoder messageEncoder = new FixMessageEncoder();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final ClaimablePublication publication;
    private final ErrorHandler errorHandler;
    private int libraryId;

    CatchupReplayer(final ClaimablePublication publication, final ErrorHandler errorHandler)
    {
        this.publication = publication;
        this.errorHandler = errorHandler;
    }

    public void onFragment(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final Header header)
    {
        if (publication.claim(length, bufferClaim) > 0)
        {
            final MutableDirectBuffer destBuffer = bufferClaim.buffer();
            final int destOffset = bufferClaim.offset();
            destBuffer.putBytes(destOffset, srcBuffer, srcOffset, length);

            final int frameOffset = destOffset + MessageHeaderEncoder.ENCODED_LENGTH;
            messageEncoder
                .wrap(destBuffer, frameOffset)
                .libraryId(libraryId);

            DebugLogger.log("Resending: %s\n", destBuffer, destOffset + FRAME_LENGTH, length - FRAME_LENGTH);

            bufferClaim.commit();
        }
        else
        {
            errorHandler.onError(new IllegalStateException(
                "Failed to claim buffer space when trying to resend " + srcBuffer.getStringUtf8(srcOffset, length)));
        }
    }

    void libraryId(final int libraryId)
    {
        this.libraryId = libraryId;
    }
}
