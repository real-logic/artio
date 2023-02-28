/*
 * Copyright 2015-2023 Real Logic Limited.
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

import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.FixPConnectedSessionInfo;

import java.util.List;

/**
 * Engine managed model of a library instance.
 */
public interface LibraryInfo
{
    /**
     * Get the id of the library.
     *
     * @return the id of the library.
     */
    int libraryId();

    /**
     * Get the debug name of the library.
     *
     * @return the debug name of the library. May be null.
     */
    String libraryName();

    /**
     * Get an unmodifiable list of the current sessions connected to this library.
     *
     * @return an unmodifiable list of the current sessions connected to this library.
     */
    List<ConnectedSessionInfo> sessions();

    List<FixPConnectedSessionInfo> fixPConnections();
}
