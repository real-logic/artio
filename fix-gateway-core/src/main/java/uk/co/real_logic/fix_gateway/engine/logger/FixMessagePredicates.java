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
package uk.co.real_logic.fix_gateway.engine.logger;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.BufferAsciiSequence;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

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
            if (predicate.test(message))
            {
                consumer.onMessage(message, buffer, offset, length, header);
            }
        };
    }

    /**
     * Filters a timestamp to be between these begin and end times.
     *
     * @return the resulting predicate
     */
    public static FixMessagePredicate between(final long beginTimestampInclusive, final long endTimestampExclusive)
    {
        return (message) ->
        {
            final long timestamp = message.timestamp();
            return timestamp >= beginTimestampInclusive && timestamp < endTimestampExclusive;
        };
    }

    /**
     * Filter messages by the message type of their fix message.
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
        final char[] expectedSenderCompId = senderCompId.toCharArray();
        final char[] expectedTargetCompId = targetCompId.toCharArray();
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
            return CodecUtil.equals(header.senderCompID(), expectedSenderCompId, header.senderCompIDLength())
                && CodecUtil.equals(header.targetCompID(), expectedTargetCompId, header.targetCompIDLength());
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
}
