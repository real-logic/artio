/*
 * Copyright 2019 Monotonic Ltd.
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
package uk.co.real_logic.artio.builder;

import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface AbstractTestRequestEncoder extends Encoder
{
    AbstractTestRequestEncoder testReqID(DirectBuffer value, int offset, int length);

    AbstractTestRequestEncoder testReqID(DirectBuffer value, int length);

    AbstractTestRequestEncoder testReqID(DirectBuffer value);

    AbstractTestRequestEncoder testReqID(byte[] value, int offset, int length);

    AbstractTestRequestEncoder testReqID(byte[] value, int length);

    AbstractTestRequestEncoder testReqID(byte[] value);

    boolean hasTestReqID();

    String testReqIDAsString();

    AbstractTestRequestEncoder testReqID(CharSequence value);

    AbstractTestRequestEncoder testReqID(AsciiSequenceView value);

    AbstractTestRequestEncoder testReqID(char[] value, int offset, int length);

    AbstractTestRequestEncoder testReqID(char[] value, int length);

    AbstractTestRequestEncoder testReqID(char[] value);

    MutableDirectBuffer testReqID();

    void resetTestReqID();
}
