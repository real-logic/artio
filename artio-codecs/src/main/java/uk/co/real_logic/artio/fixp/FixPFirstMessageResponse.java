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
     * The first message has been rejected due to invalid credentials.
     *
     * Might require either an establish or negotiate reject being sent depending upon which message you're replying to.
     */
    CREDENTIALS,

    /**
     * A Negotiate message has been rejected due to a duplicate session id.
     */
    NEGOTIATE_DUPLICATE_ID,

    /**
     * A Negotiate message has been rejected due to a duplicate session id, with a bad session version.
     *
     * This is used when comparing session versions of follower created sessions.
     */
    NEGOTIATE_DUPLICATE_ID_BAD_VER,

    /**
     * A Negotiate message has been rejected with an unspecified reason.
     */
    NEGOTIATE_UNSPECIFIED,

    /**
     * An Establish message has been rejected due to a duplicate session id.
     */
    ESTABLISH_DUPLICATE_ID,

    /**
     * An establish message has been rejected due the session not having been negotiated yet.
     */
    ESTABLISH_UNNEGOTIATED,

    /**
     * Session ver id has been ended through the normal protocol, so can't continue.
     */
    VER_ID_ENDED,
}
