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
package uk.co.real_logic.artio.decoder;

import org.agrona.AsciiSequenceView;
import uk.co.real_logic.artio.builder.Decoder;

public interface AbstractLogonDecoder extends Decoder
{
    int heartBtInt();

    boolean supportsUsername();

    char[] username();

    boolean hasUsername();

    int usernameLength();

    String usernameAsString();

    AsciiSequenceView username(AsciiSequenceView view);

    boolean supportsPassword();

    char[] password();

    boolean hasPassword();

    int passwordLength();

    String passwordAsString();

    AsciiSequenceView password(AsciiSequenceView view);

    boolean hasResetSeqNumFlag();

    boolean resetSeqNumFlag();

    boolean supportsCancelOnDisconnectType();

    boolean hasCancelOnDisconnectType();

    int cancelOnDisconnectType();

    boolean supportsCODTimeoutWindow();

    boolean hasCODTimeoutWindow();

    int cODTimeoutWindow();
}
