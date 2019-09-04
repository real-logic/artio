/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.dictionary;

public final class StandardFixConstants
{
    public static final byte START_OF_HEADER = 0x01;

    public static final int FIX4_HEADER_LENGTH = "8=FIX.4.2 ".length();
    public static final int FIXT_HEADER_LENGTH = "8=FIXT.1.1 ".length();

    // header and message length tag
    public static final int MIN_MESSAGE_SIZE = FIXT_HEADER_LENGTH + 6;

    // Message Types

    public static final int HEARTBEAT = 0;
    public static final int CHECKSUM = 10;
    public static final int MESSAGE_TYPE = 35;
}
