/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio.fields;

import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * .
 */
public final class UtcDateOnlyEncoder
{
    public static final int LENGTH = 8;
    public static final int MIN_EPOCH_DAYS = LocalMktDateDecoder.MIN_EPOCH_DAYS;
    public static final int MAX_EPOCH_DAYS = LocalMktDateDecoder.MAX_EPOCH_DAYS;

    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer();

    public int encode(final int epochDays, final byte[] bytes)
    {
        buffer.wrap(bytes);
        return encode(epochDays, buffer, 0);
    }

    public static int encode(final int epochDays, final MutableAsciiBuffer string, final int offset)
    {
        if (epochDays < MIN_EPOCH_DAYS || epochDays > MAX_EPOCH_DAYS)
        {
            throw new IllegalArgumentException(epochDays + " is outside of the valid range for this encoder");
        }

        CalendricalUtil.encodeDate(epochDays, string, offset);

        return LENGTH;
    }
}
