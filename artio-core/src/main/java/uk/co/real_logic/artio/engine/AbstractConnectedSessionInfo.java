/*
 * Copyright 2021 Monotonic Ltd.
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
 * Exposes information that an Engine is aware of about a session that is currently connected.
 * This is abstract in the sense that it could be a FIX or FIXP session.
 */
public interface AbstractConnectedSessionInfo
{
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
}
