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

public interface AbstractLogoutEncoder extends Encoder
{
    AbstractLogoutEncoder text(DirectBuffer value, int offset, int length);

    AbstractLogoutEncoder text(DirectBuffer value, int length);

    AbstractLogoutEncoder text(DirectBuffer value);

    AbstractLogoutEncoder text(byte[] value, int offset, int length);

    AbstractLogoutEncoder text(byte[] value, int length);

    AbstractLogoutEncoder text(byte[] value);

    boolean hasText();

    String textAsString();

    AbstractLogoutEncoder text(CharSequence value);

    AbstractLogoutEncoder text(AsciiSequenceView value);

    AbstractLogoutEncoder text(char[] value, int offset, int length);

    AbstractLogoutEncoder text(char[] value, int length);

    AbstractLogoutEncoder text(char[] value);

    MutableDirectBuffer text();

    void resetText();
}
