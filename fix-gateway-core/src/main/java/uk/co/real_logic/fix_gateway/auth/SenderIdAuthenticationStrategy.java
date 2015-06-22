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
package uk.co.real_logic.fix_gateway.auth;

import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class SenderIdAuthenticationStrategy implements AuthenticationStrategy
{
    private final CharArrayWrapper wrapper = new CharArrayWrapper();
    private final Set<CharArrayWrapper> validSenderIds;

    public SenderIdAuthenticationStrategy(final List<String> validSenderIds)
    {
        this.validSenderIds = validSenderIds
            .stream()
            .map(str -> new CharArrayWrapper().wrap(str))
            .collect(toSet());
    }

    public boolean authenticate(final LogonDecoder logon)
    {
        final HeaderDecoder header = logon.header();
        final char[] senderCompID = header.senderCompID();
        final int senderCompIDLength = header.senderCompIDLength();

        wrapper.wrap(senderCompID, senderCompIDLength);
        return validSenderIds.contains(wrapper);
    }

    private final class CharArrayWrapper
    {
        private char[] values;
        private int length;
        private int hashcode;

        public CharArrayWrapper wrap(final String string)
        {
            final char[] values = string.toCharArray();
            wrap(values, values.length);
            return this;
        }

        public void wrap(final char[] values, final int length)
        {
            this.values = values;
            this.length = length;
            hashcode = CodecUtil.hashCode(values, length);
        }

        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final CharArrayWrapper that = (CharArrayWrapper) o;

            final int length = this.length;
            if (length != that.length)
            {
                return false;
            }

            return CodecUtil.equals(values, that.values, length);
        }

        public int hashCode()
        {
            return hashcode;
        }
    }
}
