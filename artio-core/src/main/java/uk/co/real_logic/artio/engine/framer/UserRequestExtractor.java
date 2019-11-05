/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
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

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.decoder.AbstractUserRequestDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

/**
 * Separated out for lazy initialization so that FIX dictionaries that don't contain the UserRequest message
 * (eg: FIX 4.2) don't result in linking errors.
 */
class UserRequestExtractor
{
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final AbstractUserRequestDecoder userRequest;
    private final FixDictionary dictionary;
    private final ErrorHandler errorHandler;

    UserRequestExtractor(final FixDictionary dictionary, final ErrorHandler errorHandler)
    {
        userRequest = dictionary.makeUserRequestDecoder();
        this.dictionary = dictionary;
        this.errorHandler = errorHandler;
    }

    void onUserRequest(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final AuthenticationStrategy authenticationStrategy,
        final long connectionId,
        final long sessionId)
    {
        if (userRequest == null)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Received User Request message despite there being no user request message type defined " +
                "in dictionary (dict=%s, conn=%d, sess=%d)",
                dictionary.getClass().getName(),
                connectionId,
                sessionId)));

            return;
        }

        asciiBuffer.wrap(buffer);
        userRequest.reset();
        userRequest.decode(asciiBuffer, offset, length);

        authenticationStrategy.onUserRequest(userRequest, sessionId);
    }
}
