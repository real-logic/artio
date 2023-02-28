/*
 * Copyright 2015-2023 Real Logic Limited., Adaptive Financial Consulting Ltd., Monotonic Ltd.
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

import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.util.AsciiBuffer;

/**
 * Parent interface that all Artio decoders implement.
 */
public interface Decoder extends CharAppender
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

    /**
     * Gets the Header component of this message.
     *
     * @return the Header component of this message.
     */
    SessionHeaderDecoder header();

    /**
     * Append a JSON representation of this Decoder value to a {@link StringBuilder} object for debugging purposes.
     * Note: there is no guarantee that this internal format will be stable between versions this should only be used
     * for debugging purposes. This should be preferred to {@link Object#toString()} because it is a more efficient,
     * zero allocation, method.
     *
     * @param builder the builder to append a JSON string representation to.
     * @param level the whitespace indentation level to use.
     * @return the builder passed as an argument
     */
    StringBuilder appendTo(StringBuilder builder, int level);

    /**
     * Copies the field values on the Encoder to be the same as this Decoder. This also sets all child components and
     * repeating group values to be the same.
     *
     * This method also resets the encoder before any fields are set so that missing values within the Decoder are also
     * missing within the Encoder. If you wish to copy some of a decoder's fields and set others then the values to be
     * set should be done after this method is called.
     *
     * Care should be taken when using this method - encoders flyweight their String-like fields so the fields of the
     * Decoder will be referenced by the Encoder when
     *
     * @param encoder the encoder to set the values of.
     * @return the encoder passed as an argument.
     */
    Encoder toEncoder(Encoder encoder);
}
