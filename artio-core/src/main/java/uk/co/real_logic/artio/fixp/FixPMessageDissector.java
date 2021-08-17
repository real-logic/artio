/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.fixp;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.sbe.MessageDecoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;

import java.util.List;
import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.FIXP_BUSINESS;

public class FixPMessageDissector
{
    private final Int2ObjectHashMap<Decoder> templateIdToDecoder = new Int2ObjectHashMap<>();
    private final Logger logger;

    public FixPMessageDissector(final List<? extends MessageDecoderFlyweight> decoders)
    {
        this(FixPMessageDissector::logDefault, decoders);
    }

    public FixPMessageDissector(final Logger logger, final List<? extends MessageDecoderFlyweight> decoders)
    {
        this.logger = logger;
        for (final MessageDecoderFlyweight flyweight : decoders)
        {
            templateIdToDecoder.put(flyweight.sbeTemplateId(), new Decoder(flyweight));
        }
    }

    public void onBusinessMessage(
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean inbound)
    {
        final Decoder decoder = templateIdToDecoder.get(templateId);
        if (decoder != null)
        {
            decoder.onMessage(buffer, offset, blockLength, version, inbound);
        }
    }

    private static void logDefault(final String prefix, final Consumer<StringBuilder> appendTo)
    {
        DebugLogger.logSbeDecoder(FIXP_BUSINESS, prefix, appendTo);
    }

    public interface Logger
    {
        void log(String prefix, Consumer<StringBuilder> appendTo);
    }

    class Decoder
    {
        private final MessageDecoderFlyweight flyweight;
        private final Consumer<StringBuilder> appendTo;

        Decoder(final MessageDecoderFlyweight flyweight)
        {
            this.flyweight = flyweight;
            this.appendTo = flyweight::appendTo;
        }

        public void onMessage(
            final DirectBuffer buffer,
            final int offset,
            final int blockLength,
            final int version,
            final boolean inbound)
        {
            flyweight.wrap(buffer, offset, blockLength, version);
            logger.log(inbound ? "> " : "< ", appendTo);
        }
    }
}
