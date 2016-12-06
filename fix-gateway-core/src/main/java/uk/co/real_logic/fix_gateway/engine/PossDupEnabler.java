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
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.fix_gateway.engine.PossDupFinder.NO_ENTRY;

public class PossDupEnabler
{
    public static final byte[] POSS_DUP_FIELD = "43=Y\001".getBytes(StandardCharsets.US_ASCII);
    private static final int CHECKSUM_TAG_SIZE = 3;

    private final PossDupFinder possDupFinder = new PossDupFinder();
    private final OtfParser parser = new OtfParser(possDupFinder, new IntDictionary());
    private final MutableAsciiBuffer mutableAsciiFlyweight = new MutableAsciiBuffer();

    private final BufferClaim bufferClaim;
    private final IntPredicate claimer;
    private final Runnable onPreCommit;
    private final Consumer<String> onIllegalStateFunc;
    private final ErrorHandler errorHandler;

    public PossDupEnabler(
        final BufferClaim bufferClaim,
        final IntPredicate claimer,
        final Runnable onPreCommit,
        final Consumer<String> onIllegalStateFunc,
        final ErrorHandler errorHandler)
    {
        this.bufferClaim = bufferClaim;
        this.claimer = claimer;
        this.onPreCommit = onPreCommit;
        this.onIllegalStateFunc = onIllegalStateFunc;
        this.errorHandler = errorHandler;
    }

    public Action enablePossDupFlag(
        final DirectBuffer srcBuffer,
        final int messageOffset,
        final int messageLength,
        final int srcOffset,
        final int srcLength)
    {
        parser.onMessage(srcBuffer, messageOffset, messageLength);
        final int possDupSrcOffset = possDupFinder.possDupOffset();
        if (possDupSrcOffset == NO_ENTRY)
        {
            final int newLength = srcLength + POSS_DUP_FIELD.length;
            if (!claimer.test(newLength))
            {
                return ABORT;
            }

            try
            {
                if (addPossDupField(
                    srcBuffer, srcOffset, srcLength, messageOffset, messageLength, claimedBuffer(), claimOffset()))
                {
                    commit();
                }
                else
                {
                    onIllegalStateFunc.accept("[%s] Missing sending time field in resend request");
                    bufferClaim.abort();
                }
            }
            catch (final Exception e)
            {
                bufferClaim.abort();
                errorHandler.onError(e);
            }
        }
        else
        {
            if (!claimer.test(srcLength))
            {
                return ABORT;
            }

            try
            {
                final MutableDirectBuffer claimedBuffer = claimedBuffer();
                final int claimOffset = claimOffset();
                claimedBuffer.putBytes(claimOffset, srcBuffer, messageOffset, messageLength);
                setPossDupFlag(possDupSrcOffset, srcOffset, claimOffset, claimedBuffer);

                commit();
            }
            catch (Exception e)
            {
                bufferClaim.abort();
                errorHandler.onError(e);
            }
        }

        return CONTINUE;
    }

    private void commit()
    {
        onPreCommit.run();
        bufferClaim.commit();
    }

    private MutableDirectBuffer claimedBuffer()
    {
        return bufferClaim.buffer();
    }

    private int claimOffset()
    {
        return bufferClaim.offset();
    }

    private boolean addPossDupField(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int messageOffset,
        final int messageLength,
        final MutableDirectBuffer claimBuffer,
        final int claimOffset)
    {
        // Sending time is a required field just before the poss dup field
        final int sendingTimeSrcEnd = possDupFinder.sendingTimeEnd();
        if (sendingTimeSrcEnd == NO_ENTRY)
        {
            return false;
        }
        final int lengthToPossDup = sendingTimeSrcEnd - srcOffset;
        final int possDupClaimOffset = claimOffset + lengthToPossDup;
        final int remainingClaimOffset = possDupClaimOffset + POSS_DUP_FIELD.length;

        claimBuffer.putBytes(claimOffset, srcBuffer, srcOffset, lengthToPossDup);
        claimBuffer.putBytes(possDupClaimOffset, POSS_DUP_FIELD);
        claimBuffer.putBytes(remainingClaimOffset, srcBuffer, sendingTimeSrcEnd, srcLength - lengthToPossDup);

        updateFrameBodyLength(messageLength, claimBuffer, claimOffset);
        final int messageClaimOffset = srcToClaim(messageOffset, srcOffset, claimOffset);
        updateMessage(srcOffset, messageClaimOffset, claimBuffer, claimOffset);

        return true;
    }

    private void updateFrameBodyLength(
        final int messageLength, final MutableDirectBuffer claimBuffer, final int claimOffset)
    {
        final int frameBodyLengthOffset = claimOffset + MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH;
        final short frameBodyLength = (short) (messageLength + POSS_DUP_FIELD.length);
        claimBuffer.putShort(frameBodyLengthOffset, frameBodyLength, LITTLE_ENDIAN);
    }

    private void updateMessage(
        final int srcOffset, final int messageClaimOffset, final MutableDirectBuffer claimBuffer, int claimOffset)
    {
        mutableAsciiFlyweight.wrap(claimBuffer);

        // Update Body Length
        final int newBodyLength = possDupFinder.bodyLength() + POSS_DUP_FIELD.length;
        final int bodyLengthClaimOffset = srcToClaim(possDupFinder.bodyLengthOffset(), srcOffset, claimOffset);
        mutableAsciiFlyweight.putNatural(bodyLengthClaimOffset, possDupFinder.lengthOfBodyLength(), newBodyLength);

        updateChecksum(messageClaimOffset, newBodyLength, bodyLengthClaimOffset);
    }

    private void updateChecksum(int messageClaimOffset, int newBodyLength, int bodyLengthClaimOffset)
    {
        final int beforeChecksum = bodyLengthClaimOffset + newBodyLength + POSS_DUP_FIELD.length;
        final int checksum = mutableAsciiFlyweight.computeChecksum(messageClaimOffset, beforeChecksum);
        mutableAsciiFlyweight.putNatural(beforeChecksum + CHECKSUM_TAG_SIZE, 3, checksum);
    }

    private void setPossDupFlag(
        final int possDupSrcOffset,
        final int srcOffset,
        final int claimOffset,
        final MutableDirectBuffer claimBuffer)
    {
        final int possDupClaimOffset = srcToClaim(possDupSrcOffset, srcOffset, claimOffset);
        mutableAsciiFlyweight.wrap(claimBuffer);
        mutableAsciiFlyweight.putChar(possDupClaimOffset, 'Y');
    }

    private int srcToClaim(final int srcIndexedOffset, final int srcOffset, final int claimOffset)
    {
        return srcIndexedOffset - srcOffset + claimOffset;
    }
}
