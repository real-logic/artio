/*
 * Copyright 2020 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.decoder.AbstractSequenceResetDecoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.util.AsciiBuffer;

import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.artio.dictionary.SessionConstants.START_OF_HEADER;
import static uk.co.real_logic.artio.util.AsciiBuffer.UNKNOWN_INDEX;

class AcceptorFixDictionaryLookup
{
    // Eg: 8=FIXT.1.1
    private static final int BEGIN_STRING_OFFSET = "8=".length();

    private final FixDictionary defaultfixDictionary;
    private final Map<String, FixDictionary> fixVersionToDictionaryOverride;
    private final Map<FixDictionary, CachedDecoders> dictionaryToDecoders;

    AcceptorFixDictionaryLookup(
        final FixDictionary defaultfixDictionary, final Map<String, FixDictionary> fixVersionToDictionaryOverride)
    {
        this.defaultfixDictionary = defaultfixDictionary;
        this.fixVersionToDictionaryOverride = fixVersionToDictionaryOverride;

        dictionaryToDecoders = new HashMap<>();
    }

    FixDictionary lookup(final AsciiBuffer buffer, final int offset, final int length)
    {
        final int beginStringOffset = offset + BEGIN_STRING_OFFSET;
        final int messageEnd = offset + length;
        final int beginStringEnd = buffer.scan(beginStringOffset, messageEnd, START_OF_HEADER);
        if (beginStringEnd == UNKNOWN_INDEX)
        {
            return defaultfixDictionary;
        }

        final String beginString = buffer.getAscii(beginStringOffset, beginStringEnd - beginStringOffset);
        return fixVersionToDictionaryOverride.getOrDefault(beginString, defaultfixDictionary);
    }

    SessionHeaderDecoder lookupHeaderDecoder(final FixDictionary dictionary)
    {
        return lookupCachedDecoders(dictionary).sessionHeaderDecoder;
    }

    AbstractSequenceResetDecoder lookupSequenceResetDecoder(final FixDictionary dictionary)
    {
        return lookupCachedDecoders(dictionary).sequenceResetDecoder;
    }

    private CachedDecoders lookupCachedDecoders(final FixDictionary dictionary)
    {
        return dictionaryToDecoders.computeIfAbsent(dictionary, CachedDecoders::new);
    }

    private static final class CachedDecoders
    {
        private final SessionHeaderDecoder sessionHeaderDecoder;
        private final AbstractSequenceResetDecoder sequenceResetDecoder;

        private CachedDecoders(final FixDictionary dictionary)
        {
            sessionHeaderDecoder = dictionary.makeHeaderDecoder();
            sequenceResetDecoder = dictionary.makeSequenceResetDecoder();
        }
    }

}
