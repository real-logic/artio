/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.dictionary.generation.GenerationUtil;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.BufferAsciiSequence;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Filters to be used in conjunction with {@link FixArchiveScanner}.
 */
public final class FixMessagePredicates
{
    private FixMessagePredicates()
    {
    }

    /**
     * Filter messages passed to consumer, only passing through messages that pass the predicate.
     *
     * @param consumer  the consumer to receive filtered messages.
     * @param predicate the predicate to filter messages.
     * @return a new composed consumer.
     */
    public static FixMessageConsumer filterBy(final FixMessageConsumer consumer, final FixMessagePredicate predicate)
    {
        return (message, buffer, offset, length, header) ->
        {
            final int actingVersion = message.sbeSchemaVersion();
            final int actingBlockLength = message.sbeBlockLength();

            if (predicate.test(message))
            {
                // Rewrap incase the predicate.test() method has altered the limit()
                message.wrap(buffer, offset, actingBlockLength, actingVersion);
                consumer.onMessage(message, buffer, offset, length, header);
            }
        };
    }

    /**
     * Filters a timestamp to be between these begin and end times.
     *
     * Timestamps filtered in precision of CommonConfiguration.clock().
     *
     * @return the resulting predicate
     */
    public static FixMessagePredicate between(
        final long beginTimestampInclusive,
        final long endTimestampExclusive)
    {
        return from(beginTimestampInclusive).and(to(endTimestampExclusive));
    }

    /**
     * Filters a timestamp from a given begin time.
     *
     * @return the resulting predicate
     */
    public static FixMessagePredicate from(final long beginTimestampInclusive)
    {
        return (message) -> message.timestamp() >= beginTimestampInclusive;
    }

    /**
     * Filters a timestamp to a given end time.
     *
     * Timestamps filtered in precision of CommonConfiguration.clock().
     *
     * @return the resulting predicate
     */
    public static FixMessagePredicate to(final long endTimestampExclusive)
    {
        return (message) -> message.timestamp() < endTimestampExclusive;
    }

    /**
     * Filter messages by the message type of their fix message.
     *
     * Timestamps filtered in precision of CommonConfiguration.clock().
     *
     * @param messageTypes the fix message type strings that you see in the message.
     * @return the resulting predicate
     */
    public static FixMessagePredicate messageTypeOf(final String... messageTypes)
    {
        final IntHashSet hashSet = new IntHashSet();
        Stream.of(messageTypes)
            .mapToInt(GenerationUtil::packMessageType)
            .forEach(hashSet::add);
        return messageTypeOf(hashSet);
    }

    /**
     * Filter messages by the message type of their fix message.
     *
     * @param messageTypes the fix message types encoded as packed ints.
     * @return the resulting predicate.
     */
    public static FixMessagePredicate messageTypeOf(final int... messageTypes)
    {
        final IntHashSet hashSet = new IntHashSet();
        IntStream.of(messageTypes)
            .forEach(hashSet::add);
        return messageTypeOf(hashSet);
    }

    private static FixMessagePredicate messageTypeOf(final IntHashSet hashSet)
    {
        return (message) -> hashSet.contains(message.messageType());
    }

    /**
     * Filter the fix message predicate by parsing the sender and target comp ids out of the message body.
     *
     * @param senderCompId the sender comp id required in the message.
     * @param targetCompId the target comp id required in the message.
     * @return the resulting predicate.
     */
    public static FixMessagePredicate sessionOf(
        final String senderCompId,
        final String targetCompId)
    {
        return whereHeader(senderCompIdOf(senderCompId).and(targetCompIdOf(targetCompId)));
    }

    public static Predicate<HeaderDecoder> senderCompIdOf(final String senderCompId)
    {
        return headerMatches(senderCompId, HeaderDecoder::senderCompID, HeaderDecoder::senderCompIDLength);
    }

    public static Predicate<HeaderDecoder> targetCompIdOf(final String targetCompId)
    {
        return headerMatches(targetCompId, HeaderDecoder::targetCompID, HeaderDecoder::targetCompIDLength);
    }

    public static Predicate<HeaderDecoder> senderSubIdOf(final String senderSubId)
    {
        return headerMatches(senderSubId, HeaderDecoder::senderSubID, HeaderDecoder::senderSubIDLength);
    }

    public static Predicate<HeaderDecoder> targetSubIdOf(final String targetSubId)
    {
        return headerMatches(targetSubId, HeaderDecoder::targetSubID, HeaderDecoder::targetSubIDLength);
    }

    public static Predicate<HeaderDecoder> senderLocationIdOf(final String senderLocationId)
    {
        return headerMatches(senderLocationId, HeaderDecoder::senderLocationID, HeaderDecoder::senderLocationIDLength);
    }

    public static Predicate<HeaderDecoder> targetLocationIdOf(final String targetLocationId)
    {
        return headerMatches(targetLocationId, HeaderDecoder::targetLocationID, HeaderDecoder::targetLocationIDLength);
    }

    public static Predicate<HeaderDecoder> headerMatches(
        final String value,
        final Function<HeaderDecoder, char[]> charExtractor,
        final ToIntFunction<HeaderDecoder> lengthExtractor)
    {
        final char[] expectedChars = value.toCharArray();
        return header ->
        {
            final char[] actualChars = charExtractor.apply(header);
            final int length = lengthExtractor.applyAsInt(header);
            return CodecUtil.equals(actualChars, expectedChars, length);
        };
    }

    public static FixMessagePredicate whereHeader(
        final Predicate<HeaderDecoder> matches)
    {
        final HeaderDecoder header = new HeaderDecoder();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
        final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
        return message ->
        {
            final int length = message.bodyLength();
            buffer.checkLimit(length);
            message.getBody(buffer, 0, length);
            asciiBuffer.wrap(buffer);
            header.decode(asciiBuffer, 0, length);
            return matches.test(header);
        };
    }

    /**
     * Filter the fix message by checking the assigned session id field is equal to the given parameter.
     *
     * @param sessionId the surrogate session id key.
     * @return the resulting predicate.
     */
    public static FixMessagePredicate sessionOf(final long sessionId)
    {
        return (message) -> message.session() == sessionId;
    }

    public static FixMessagePredicate bodyMatches(final Pattern pattern)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
        final BufferAsciiSequence sequence = new BufferAsciiSequence();
        return message ->
        {
            final int length = message.bodyLength();
            buffer.checkLimit(length);
            message.getBody(buffer, 0, length);
            sequence.wrap(buffer, 0, length);
            return pattern.matcher(sequence).matches();
        };
    }

    public static FixMessagePredicate alwaysTrue()
    {
        return message -> true;
    }
}
