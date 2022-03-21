/*
 * Copyright 2015-2020 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine;

import uk.co.real_logic.artio.session.CompositeKey;

/**
 * Expose information about an Artio FIX Session.
 */
public interface SessionInfo
{
    /**
     * The used to identify a sequenceIndex that hasn't yet been identified
     */
    int UNKNOWN_SEQUENCE_INDEX = -1;

    /**
     * The used to identify a session that hasn't yet been identified
     */
    int UNK_SESSION = -1;

    /**
     * Get the identification number of the session in question or {@link #UNK_SESSION}
     * if the session hasn't completed its logon yet.
     *
     * @return the session id.
     */
    long sessionId();

    /**
     * Get the full identifying key of the session in question or <code>null</code>
     * if the session hasn't completed its logon yet.
     *
     * @return the full identifying key of the session in question.
     */
    CompositeKey sessionKey();

    /**
     * Get the session sequence index or {@link #UNKNOWN_SEQUENCE_INDEX}
     * if the session hasn't completed its logon yet.
     *
     * @return the sequenceIndex
     */
    int sequenceIndex();
}
