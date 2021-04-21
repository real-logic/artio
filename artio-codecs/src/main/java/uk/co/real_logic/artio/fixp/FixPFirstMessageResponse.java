/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.fixp;

/**
 * Enum for representing common negotiate and establish reject reasons and codes between different FIXP codec
 * implementations
 */
public enum FixPFirstMessageResponse
{
    /** No problem has been identified for the first message */
    OK,
    /**
     * Might require either an establish or negotiate reject being sent depending upon which message you're replying to
     */
    CREDENTIALS,
    NEGOTIATE_DUPLICATE_ID,
    NEGOTIATE_UNSPECIFIED,
    ESTABLISH_UNNEGOTIATED,
}
