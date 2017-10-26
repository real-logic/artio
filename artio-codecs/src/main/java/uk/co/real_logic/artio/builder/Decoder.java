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
package uk.co.real_logic.artio.builder;

import uk.co.real_logic.artio.util.AsciiBuffer;

public interface Decoder
{
    int NO_ERROR = -1;

    int decode(AsciiBuffer buffer, int offset, int length);

    /**
     * Resets the result of the decoder.
     */
    void reset();

    /**
     * If Validation is switched on then returns it true if the message is valid, false otherwise. If
     * Validation is switched off then the result of this is undefined.
     *
     * @return true if the message is valid, false otherwise
     */
    boolean validate();

    /**
     * You must call {@link #validate()} first before relying on the result of this method.
     *
     * @return the tag id of the tag that failed validation, or {@code NO_ERROR} if there's no error.
     */
    int invalidTagId();

    /**
     * You must call {@link #validate()} first before relying on the result of this method.
     *
     * This method doesn't return an enum in order to avoid cyclic compilation dependencies upon generated enums.
     *
     * @return the session reject reason error code corresponding to the validation error,
     * or {@code NO_ERROR} if there's no error.
     */
    int rejectReason();
}
