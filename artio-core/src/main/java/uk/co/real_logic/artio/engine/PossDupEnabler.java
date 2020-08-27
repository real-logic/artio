/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.engine;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.dictionary.LongDictionary;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.otf.OtfParser;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_FLAG;
import static io.aeron.protocol.DataHeaderFlyweight.END_FLAG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static uk.co.real_logic.artio.engine.PossDupFinder.NO_ENTRY;
import static uk.co.real_logic.artio.engine.framer.CatchupReplayer.FRAME_LENGTH;
import static uk.co.real_logic.artio.messages.FixMessageDecoder.metaDataHeaderLength;
import static uk.co.real_logic.artio.util.AsciiBuffer.SEPARATOR_LENGTH;
import static uk.co.real_logic.artio.util.MutableAsciiBuffer.SEPARATOR;

public class PossDupEnabler
{
    private static final byte[] POSS_DUP_FIELD = "43=Y\001".getBytes(US_ASCII);
    public static final String ORIG_SENDING_TIME_PREFIX_AS_STR = "122=";
    private static final byte[] ORIG_SENDING_TIME_PREFIX = ORIG_SENDING_TIME_PREFIX_AS_STR.getBytes(US_ASCII);

    private static final int CHECKSUM_VALUE_LENGTH = 3;
    private static final int FRAGMENTED_MESSAGE_BUFFER_OFFSET = 0;

    private final ExpandableArrayBuffer fragmentedMessageBuffer = new ExpandableArrayBuffer();
    private final PossDupFinder possDupFinder = new PossDupFinder();
    private final OtfParser parser = new OtfParser(possDupFinder, new LongDictionary());
    private final MutableAsciiBuffer mutableAsciiFlyweight = new MutableAsciiBuffer();
    private final UtcTimestampEncoder utcTimestampEncoder;

    private final BufferClaim bufferClaim;
    private final Claimer claimer;
    private final PreCommit onPreCommit;
    private final Consumer<String> onIllegalStateFunc;
    private final ErrorHandler errorHandler;
    private final EpochClock clock;
    private final int maxPayloadLength;
    private final LogTag logTag;

    private int fragmentedMessageLength;

    public interface Claimer
    {
        boolean claim(int totalLength, int messageLength);
    }

    public PossDupEnabler(
        final UtcTimestampEncoder utcTimestampEncoder,
        final BufferClaim bufferClaim,
        final Claimer claimer,
        final PreCommit onPreCommit,
        final Consumer<String> onIllegalStateFunc,
        final ErrorHandler errorHandler,
        final EpochClock clock,
        final int maxPayloadLength,
        final LogTag logTag)
    {
        this.utcTimestampEncoder = utcTimestampEncoder;
        this.bufferClaim = bufferClaim;
        this.claimer = claimer;
        this.onPreCommit = onPreCommit;
        this.onIllegalStateFunc = onIllegalStateFunc;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.maxPayloadLength = maxPayloadLength;
        this.logTag = logTag;
    }

    // Only return abort if genuinely back pressured
    public Action enablePossDupFlag(
        final DirectBuffer srcBuffer,
        final int messageOffset,
        final int messageLength,
        final int srcOffset,
        final int srcLength,
        final int metaDataAdjustment)
    {
        parser.onMessage(srcBuffer, messageOffset, messageLength);
        final int possDupSrcOffset = possDupFinder.possDupOffset();
        if (possDupSrcOffset == NO_ENTRY)
        {
            final int lengthOfOldBodyLength = possDupFinder.lengthOfBodyLength();
            final int lengthOfAddedFields = POSS_DUP_FIELD.length +
                ORIG_SENDING_TIME_PREFIX.length +
                possDupFinder.sendingTimeLength() +
                SEPARATOR_LENGTH;
            int newLength = srcLength + lengthOfAddedFields;
            final int newBodyLength = possDupFinder.bodyLength() + lengthOfAddedFields;
            final int lengthOfNewBodyLength = MutableAsciiBuffer.lengthInAscii(newBodyLength);
            // Account for having to resize the body length field
            // Might be smaller due to padding
            final int lengthDelta = Math.max(0, lengthOfNewBodyLength - lengthOfOldBodyLength);
            newLength += lengthDelta;

            if (!claim(newLength))
            {
                return ABORT;
            }

            try
            {
                if (addFields(
                    srcBuffer,
                    srcOffset,
                    srcLength,
                    messageOffset,
                    messageLength,
                    lengthDelta + lengthOfAddedFields,
                    newBodyLength,
                    newLength,
                    metaDataAdjustment))
                {
                    return commit(true);
                }
                else
                {
                    onIllegalStateFunc.accept("[%s] Missing sending time field in resend request");
                    abort();
                }
            }
            catch (final Exception ex)
            {
                abort();
                errorHandler.onError(ex);
            }
        }
        else
        {
            if (!claim(srcLength))
            {
                return ABORT;
            }

            try
            {
                final MutableDirectBuffer writeBuffer = writeBuffer();
                final int writeOffset = writeOffset();
                writeBuffer.putBytes(writeOffset, srcBuffer, messageOffset, messageLength);
                setPossDupFlag(possDupSrcOffset, messageOffset, writeOffset, writeBuffer);
                updateSendingTime(messageOffset);

                return commit(false);
            }
            catch (final Exception ex)
            {
                abort();
                errorHandler.onError(ex);
            }
        }

        return CONTINUE;
    }

    private void abort()
    {
        if (isProcessingFragmentedMessage())
        {
            fragmentedMessageLength = 0;
        }
        else
        {
            bufferClaim.abort();
        }
    }

    private boolean claim(final int newLength)
    {
        if (newLength > maxPayloadLength)
        {
            fragmentedMessageBuffer.checkLimit(newLength);
            fragmentedMessageLength = newLength;
            return true;
        }
        else
        {
            final int messageLength = newLength - GatewayPublication.FRAMED_MESSAGE_SIZE;
            return claimer.claim(newLength, messageLength);
        }
    }

    private Action commit(final boolean hasAlteredBodyLength)
    {
        final int logLengthOffset = hasAlteredBodyLength ? FRAME_LENGTH + metaDataHeaderLength() : 0;
        if (isProcessingFragmentedMessage())
        {
            int fragmentOffset = FRAGMENTED_MESSAGE_BUFFER_OFFSET;
            onPreCommit.onPreCommit(fragmentedMessageBuffer, fragmentOffset);

            DebugLogger.log(
                logTag,
                "Resending: ",
                fragmentedMessageBuffer,
                fragmentOffset + logLengthOffset,
                fragmentedMessageLength - logLengthOffset);

            while (fragmentedMessageLength > 0)
            {
                final int fragmentLength = Math.min(maxPayloadLength, fragmentedMessageLength);
                final int messageLength = fragmentLength - GatewayPublication.FRAMED_MESSAGE_SIZE;
                if (claimer.claim(fragmentLength, messageLength))
                {
                    if (fragmentOffset == FRAGMENTED_MESSAGE_BUFFER_OFFSET)
                    {
                        bufferClaim.flags((byte)BEGIN_FLAG);
                    }
                    else if (fragmentedMessageLength == fragmentLength)
                    {
                        bufferClaim.flags((byte)END_FLAG);
                    }
                    else
                    {
                        bufferClaim.flags((byte)0);
                    }

                    final MutableDirectBuffer destBuffer = bufferClaim.buffer();
                    final int destOffset = bufferClaim.offset();

                    destBuffer.putBytes(destOffset, fragmentedMessageBuffer, fragmentOffset, fragmentLength);
                    bufferClaim.commit();

                    fragmentOffset += fragmentLength;
                    fragmentedMessageLength -= fragmentLength;
                }
                else
                {
                    fragmentedMessageLength = 0;
                    return ABORT;
                }
            }
        }
        else
        {
            final MutableDirectBuffer buffer = bufferClaim.buffer();
            final int offset = bufferClaim.offset();

            DebugLogger.log(
                logTag,
                "Resending: ",
                buffer,
                offset + logLengthOffset,
                bufferClaim.length() - logLengthOffset);

            onPreCommit.onPreCommit(buffer, offset);
            bufferClaim.commit();
        }

        return CONTINUE;
    }

    private MutableDirectBuffer writeBuffer()
    {
        return isProcessingFragmentedMessage() ? fragmentedMessageBuffer : bufferClaim.buffer();
    }

    private int writeOffset()
    {
        return isProcessingFragmentedMessage() ? FRAGMENTED_MESSAGE_BUFFER_OFFSET : bufferClaim.offset();
    }

    private boolean addFields(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int messageOffset,
        final int messageLength,
        final int totalLengthDelta,
        final int newBodyLength,
        final int newLength,
        final int metaDataAdjustment)
    {
        final MutableDirectBuffer writeBuffer = writeBuffer();
        final int writeOffset = writeOffset();

        // Sending time is a required field just before the poss dup field
        final int sendingTimeSrcEnd = possDupFinder.sendingTimeEnd();
        if (sendingTimeSrcEnd == NO_ENTRY)
        {
            return false;
        }

        // Put messages up to the end of sending time
        final int lengthToPossDup = sendingTimeSrcEnd - srcOffset;
        writeBuffer.putBytes(writeOffset, srcBuffer, srcOffset, lengthToPossDup);

        // Insert Poss Dup Field
        final int possDupClaimOffset = writeOffset + lengthToPossDup;
        writeBuffer.putBytes(possDupClaimOffset, POSS_DUP_FIELD);

        // Insert Orig Sending Time Field
        final int origSendingTimePrefixClaimOffset = possDupClaimOffset + POSS_DUP_FIELD.length;
        writeBuffer.putBytes(origSendingTimePrefixClaimOffset, ORIG_SENDING_TIME_PREFIX);

        final int origSendingTimeValueClaimOffset = origSendingTimePrefixClaimOffset + ORIG_SENDING_TIME_PREFIX.length;
        final int sendingTimeOffset = possDupFinder.sendingTimeOffset();
        final int sendingTimeLength = possDupFinder.sendingTimeLength();
        writeBuffer.putBytes(origSendingTimeValueClaimOffset, srcBuffer, sendingTimeOffset, sendingTimeLength);

        final int separatorClaimOffset = origSendingTimeValueClaimOffset + sendingTimeLength;
        writeBuffer.putByte(separatorClaimOffset, SEPARATOR);

        // Insert the rest of the message
        final int remainingClaimOffset = separatorClaimOffset + SEPARATOR_LENGTH;
        final int remainingLength = srcLength - lengthToPossDup;
        writeBuffer.putBytes(remainingClaimOffset, srcBuffer, sendingTimeSrcEnd, remainingLength);

        // Update the sending time
        updateSendingTime(srcOffset);

        updateFrameBodyLength(messageLength, writeBuffer, writeOffset, totalLengthDelta, metaDataAdjustment);
        final int messageClaimOffset = srcToClaim(messageOffset, srcOffset, writeOffset);
        updateBodyLengthAndChecksum(
            srcOffset, messageClaimOffset, writeBuffer, writeOffset, newBodyLength, writeOffset + newLength);

        return true;
    }

    private void updateSendingTime(final int srcOffset)
    {
        final MutableDirectBuffer claimBuffer = writeBuffer();
        final int claimOffset = writeOffset();
        final int sendingTimeOffset = possDupFinder.sendingTimeOffset();
        final int sendingTimeLength = possDupFinder.sendingTimeLength();

        final int sendingTimeClaimOffset = srcToClaim(sendingTimeOffset, srcOffset, claimOffset);
        utcTimestampEncoder.encodeFrom(clock.time(), TimeUnit.MILLISECONDS);
        claimBuffer.putBytes(sendingTimeClaimOffset, utcTimestampEncoder.buffer(), 0, sendingTimeLength);
    }

    private void updateFrameBodyLength(
        final int messageLength,
        final MutableDirectBuffer claimBuffer,
        final int claimOffset,
        final int lengthDelta,
        final int metaDataAdjustment)
    {
        final int frameBodyLengthOffset =
            claimOffset + MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + metaDataAdjustment;
        final short frameBodyLength = (short)(messageLength + lengthDelta);
        claimBuffer.putShort(frameBodyLengthOffset, frameBodyLength, LITTLE_ENDIAN);
    }

    private void updateBodyLengthAndChecksum(
        final int srcOffset,
        final int messageClaimOffset,
        final MutableDirectBuffer claimBuffer,
        final int claimOffset,
        final int newBodyLength,
        final int messageEndOffset)
    {
        mutableAsciiFlyweight.wrap(claimBuffer);

        // BEGIN Update body length
        final int bodyLengthClaimOffset = srcToClaim(possDupFinder.bodyLengthOffset(), srcOffset, claimOffset);
        final int lengthOfOldBodyLength = possDupFinder.lengthOfBodyLength();
        final int lengthOfNewBodyLength = MutableAsciiBuffer.lengthInAscii(newBodyLength);

        final int lengthChange = lengthOfNewBodyLength - lengthOfOldBodyLength;
        if (lengthChange > 0)
        {
            final int index = bodyLengthClaimOffset + lengthChange;
            mutableAsciiFlyweight.putBytes(
                index,
                mutableAsciiFlyweight,
                bodyLengthClaimOffset,
                messageEndOffset - index);
        }
        // Max to avoid special casing the prefixing of the field with zeros
        final int lengthOfUpdatedBodyLengthField = Math.max(lengthOfOldBodyLength, lengthOfNewBodyLength);
        mutableAsciiFlyweight.putNaturalPaddedIntAscii(
            bodyLengthClaimOffset, lengthOfUpdatedBodyLengthField, newBodyLength);
        // END Update body length

        final int beforeChecksum = bodyLengthClaimOffset + lengthOfUpdatedBodyLengthField + newBodyLength;
        updateChecksum(messageClaimOffset, beforeChecksum, messageEndOffset);
    }

    private void updateChecksum(final int messageClaimOffset, final int beforeChecksum, final int messageEndOffset)
    {
        final int lengthOfSeparator = 1;
        final int checksumEnd = beforeChecksum + lengthOfSeparator;
        final int checksum = mutableAsciiFlyweight.computeChecksum(messageClaimOffset, checksumEnd);
        final int checksumValueOffset = messageEndOffset - (CHECKSUM_VALUE_LENGTH + SEPARATOR_LENGTH);
        mutableAsciiFlyweight.putNaturalPaddedIntAscii(checksumValueOffset, CHECKSUM_VALUE_LENGTH, checksum);
        mutableAsciiFlyweight.putSeparator(checksumValueOffset + CHECKSUM_VALUE_LENGTH);
    }

    private void setPossDupFlag(
        final int possDupSrcOffset,
        final int messageOffset,
        final int claimOffset,
        final MutableDirectBuffer claimBuffer)
    {
        final int possDupClaimOffset = srcToClaim(possDupSrcOffset, messageOffset, claimOffset);
        mutableAsciiFlyweight.wrap(claimBuffer);
        mutableAsciiFlyweight.putChar(possDupClaimOffset, 'Y');
    }

    private int srcToClaim(final int srcIndexedOffset, final int srcOffset, final int claimOffset)
    {
        return srcIndexedOffset - srcOffset + claimOffset;
    }

    private boolean isProcessingFragmentedMessage()
    {
        return fragmentedMessageLength > 0;
    }

    public interface PreCommit
    {
        void onPreCommit(MutableDirectBuffer buffer, int offset);
    }
}
