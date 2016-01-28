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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.decoder.ResendRequestDecoder;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.streams.DataSubscriber;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.nio.charset.StandardCharsets;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.fix_gateway.engine.logger.PossDupFinder.NO_ENTRY;

/**
 * The replayer responds to resend requests with data from the log of sent messages.
 *
 * This agent subscribes to the stream of incoming fix data messages. It parses
 * Resend Request messages and searches the log, using the replay index to find
 * relevant messages to resend.
 */
public class Replayer implements SessionHandler, FragmentHandler, Agent
{
    public static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    public static final int SIZE_OF_LENGTH_FIELD = 2;
    public static final byte[] POSS_DUP_FIELD = "43=Y\001".getBytes(StandardCharsets.US_ASCII);
    public static final int POLL_LIMIT = 10;
    public static final int CHECKSUM_TAG_SIZE = 3;

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();

    // Used in onMessage
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    // Used in when updating the poss dup field
    private final MutableAsciiBuffer mutableAsciiFlyweight = new MutableAsciiBuffer();

    private final Subscription subscription;
    private final ReplayQuery replayQuery;
    private final Publication publication;
    private final BufferClaim claim;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;

    private final PossDupFinder possDupFinder = new PossDupFinder();
    private final OtfParser parser = new OtfParser(possDupFinder, new IntDictionary());
    private final DataSubscriber dataSubscriber = new DataSubscriber(this);

    private int currentMessageOffset;
    private int currentMessageLength;

    public Replayer(
        final Subscription subscription,
        final ReplayQuery replayQuery,
        final Publication publication,
        final BufferClaim claim,
        final IdleStrategy idleStrategy,
        final ErrorHandler errorHandler,
        final int maxClaimAttempts)
    {
        this.subscription = subscription;
        this.replayQuery = replayQuery;
        this.publication = publication;
        this.claim = claim;
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.maxClaimAttempts = maxClaimAttempts;
    }

    public void onMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int messageType,
        final long timestamp)
    {
        if (messageType == ResendRequestDecoder.MESSAGE_TYPE)
        {
            length = Math.min(length, srcBuffer.capacity() - srcOffset);

            asciiBuffer.wrap(srcBuffer);
            currentMessageOffset = srcOffset;
            currentMessageLength = length;
            resendRequest.decode(asciiBuffer, srcOffset, length);

            final int beginSeqNo = resendRequest.beginSeqNo();
            final int endSeqNo = resendRequest.endSeqNo();
            if (endSeqNo < beginSeqNo)
            {
                onIllegalState(
                    "[%s] Error in resend request, endSeqNo (%d) < beginSeqNo (%d)",
                    message(), endSeqNo, beginSeqNo);
                return;
            }

            final int expectedCount = endSeqNo - beginSeqNo + 1;
            final int count = replayQuery.query(this, sessionId, beginSeqNo, endSeqNo);
            if (count != expectedCount)
            {
                onIllegalState(
                    "[%s] Error in resend request, count(%d) < expectedCount (%d)",
                    message(), count, expectedCount);
            }
        }
    }

    public void onFragment(
        final DirectBuffer srcBuffer, final int srcOffset, final int length, final Header header)
    {
        final int messageOffset = srcOffset + MESSAGE_FRAME_BLOCK_LENGTH;
        final int messageLength = length - MESSAGE_FRAME_BLOCK_LENGTH;

        parser.onMessage(srcBuffer, messageOffset, messageLength);
        final int possDupSrcOffset = possDupFinder.possDupOffset();
        if (possDupSrcOffset == NO_ENTRY)
        {
            final int fullLength = (messageOffset - srcOffset) + messageLength;
            final int newLength = fullLength + POSS_DUP_FIELD.length;
            if (!claimBuffer(newLength))
            {
                onIllegalState("[%s] unable to resend", message());
                return;
            }

            try
            {
                if (addPossDupField(
                    srcBuffer, srcOffset, fullLength, messageOffset, messageLength, claim.buffer(), claim.offset()))
                {
                    claim.commit();
                }
                else
                {
                    onIllegalState("[%s] Missing sending time field in resend request", message());
                    claim.abort();
                }
            }
            catch (Exception e)
            {
                claim.abort();
                onException(e);
            }
        }
        else
        {
            if (!claimBuffer(messageLength))
            {
                onIllegalState("[%s] unable to resend", message());
                return;
            }

            try
            {
                final MutableDirectBuffer claimBuffer = claim.buffer();
                final int claimOffset = claim.offset();
                claimBuffer.putBytes(claimOffset, srcBuffer, srcOffset, messageLength);
                setPossDupFlag(srcOffset, possDupSrcOffset, claimBuffer, claimOffset);

                claim.commit();
            }
            catch (Exception e)
            {
                claim.abort();
                onException(e);
            }
        }
    }

    private void onException(final Exception e)
    {
        final String message = String.format("[%s] Error replying to message", message());
        errorHandler.onError(new IllegalArgumentException(message, e));
    }

    private void onIllegalState(final String message, final Object... arguments)
    {
        errorHandler.onError(new IllegalStateException(String.format(message, arguments)));
    }

    private String message()
    {
        return asciiBuffer.getAscii(currentMessageOffset, currentMessageLength);
    }

    private boolean claimBuffer(final int newLength)
    {
        for (int i = 0; i < maxClaimAttempts; i++)
        {
            if (publication.tryClaim(newLength, claim) > 0)
            {
                return true;
            }

            idleStrategy.idle();
        }

        return false;
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

    private int updateBodyLength(int srcOffset, int claimOffset)
    {
        final int newBodyLength = possDupFinder.bodyLength() + POSS_DUP_FIELD.length;
        final int bodyLengthOffset = srcToClaim(possDupFinder.bodyLengthOffset(), srcOffset, claimOffset);
        mutableAsciiFlyweight.putNatural(bodyLengthOffset, possDupFinder.lengthOfBodyLength(), newBodyLength);
        return newBodyLength;
    }

    private void setPossDupFlag(
        final int srcOffset,
        final int possDupSrcOffset,
        final MutableDirectBuffer claimBuffer,
        final int claimOffset)
    {
        final int possDupClaimOffset = srcToClaim(possDupSrcOffset, srcOffset, claimOffset);
        mutableAsciiFlyweight.wrap(claimBuffer);
        mutableAsciiFlyweight.putChar(possDupClaimOffset, 'Y');
    }

    private int srcToClaim(final int srcIndexedOffset, final int srcOffset, final int claimOffset)
    {
        return srcIndexedOffset - srcOffset + claimOffset;
    }

    public int doWork() throws Exception
    {
        return subscription.poll(dataSubscriber, POLL_LIMIT);
    }

    public void onClose()
    {
        publication.close();
        replayQuery.close();
    }

    public String roleName()
    {
        return "Replayer";
    }
}
