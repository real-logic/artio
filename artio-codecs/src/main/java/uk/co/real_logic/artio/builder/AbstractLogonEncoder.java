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

public interface AbstractLogonEncoder extends Encoder
{
    AbstractLogonEncoder heartBtInt(int value);

    AbstractLogonEncoder resetSeqNumFlag(boolean value);

    AbstractLogonEncoder encryptMethod(int value);

    boolean supportsUsername();

    AbstractLogonEncoder username(DirectBuffer value, int offset, int length);

    AbstractLogonEncoder username(DirectBuffer value, int length);

    AbstractLogonEncoder username(DirectBuffer value);

    AbstractLogonEncoder username(byte[] value, int offset, int length);

    AbstractLogonEncoder username(byte[] value, int length);

    AbstractLogonEncoder username(byte[] value);

    boolean hasUsername();

    String usernameAsString();

    AbstractLogonEncoder username(CharSequence value);

    AbstractLogonEncoder username(AsciiSequenceView value);

    AbstractLogonEncoder username(char[] value, int offset, int length);

    AbstractLogonEncoder username(char[] value, int length);

    AbstractLogonEncoder username(char[] value);

    MutableDirectBuffer username();

    void resetUsername();

    boolean supportsPassword();

    AbstractLogonEncoder password(DirectBuffer value, int offset, int length);

    AbstractLogonEncoder password(DirectBuffer value, int length);

    AbstractLogonEncoder password(DirectBuffer value);

    AbstractLogonEncoder password(byte[] value, int offset, int length);

    AbstractLogonEncoder password(byte[] value, int length);

    AbstractLogonEncoder password(byte[] value);

    boolean hasPassword();

    String passwordAsString();

    AbstractLogonEncoder password(CharSequence value);

    AbstractLogonEncoder password(AsciiSequenceView value);

    AbstractLogonEncoder password(char[] value, int offset, int length);

    AbstractLogonEncoder password(char[] value, int length);

    AbstractLogonEncoder password(char[] value);

    MutableDirectBuffer password();

    void resetPassword();

    boolean resetSeqNumFlag();

    boolean supportsCancelOnDisconnectType();

    boolean supportsCODTimeoutWindow();

    AbstractLogonEncoder cancelOnDisconnectType(int cancelOnDisconnectType);

    AbstractLogonEncoder cODTimeoutWindow(int cODTimeoutWindow);
}
