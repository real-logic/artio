/*
 * Copyright 2015-2024 Real Logic Limited.
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

/**
 * Exposes information that an Engine is aware of about a FIX session that is currently connected.
 */
public interface ConnectedSessionInfo extends SessionInfo, AbstractConnectedSessionInfo
{
    /**
     * Returns the number of bytes outstanding in the slow consumer buffer to send.
     *
     * @return number of bytes outstanding in the slow consumer buffer to send.
     */
    long bytesInBuffer();
}
