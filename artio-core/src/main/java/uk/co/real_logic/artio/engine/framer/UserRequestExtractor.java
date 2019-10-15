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
import uk.co.real_logic.artio.builder.Validation;
import uk.co.real_logic.artio.decoder.UserRequestDecoder;
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
    private final UserRequestDecoder userRequest = new UserRequestDecoder();

    void onUserRequest(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final AuthenticationStrategy authenticationStrategy)
    {
        asciiBuffer.wrap(buffer);
        userRequest.reset();
        userRequest.decode(asciiBuffer, offset, length);

        if (Validation.CODEC_VALIDATION_ENABLED)
        {
            if (!userRequest.validate())
            {
                return;
            }
        }

        if (userRequest.hasNewPassword() && userRequest.hasPassword())
        {
            authenticationStrategy.onUserRequest(
                userRequest.password(),
                userRequest.passwordLength(),
                userRequest.newPassword(),
                userRequest.newPasswordLength());
        }
    }
}
