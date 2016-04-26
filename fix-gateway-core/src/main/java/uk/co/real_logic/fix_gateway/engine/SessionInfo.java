/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine;

/**
 * Class represents information that a gateway is aware of about a session.
 */
public interface SessionInfo
{
    /**
     * The used to identify a session that hasn't yet been identified
     */
    int UNKNOWN_SESSION = -1;

    /**
     * Get the identification number of the connection in question.
     *
     * @return the identification number of the connection in question.
     */
    long connectionId();

    /**
     * Get the remove address to which this session is connected.
     *
     * @return the remove address to which this session is connected.
     */
    String address();

    /**
     * Get the identification number of the session in question or {@link #UNKNOWN_SESSION}
     * if the session hasn't completed its logon yet.
     *
     * @return the session id.
     */
    long sessionId();

    /**
     * Returns the number of bytes outstanding in the quarantine buffer to send.
     *
     * @return number of bytes outstanding in the quarantine buffer to send.
     */
    long bytesInBuffer();
}
