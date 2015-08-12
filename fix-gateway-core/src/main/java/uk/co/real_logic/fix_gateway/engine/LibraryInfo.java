/*
 * Copyright 2015 Real Logic Ltd.
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

public final class LibraryInfo
{
    private final boolean acceptor;
    private final int libraryId;

    public LibraryInfo(final boolean acceptor, final int libraryId)
    {
        this.acceptor = acceptor;
        this.libraryId = libraryId;
    }

    public boolean isAcceptor()
    {
        return acceptor;
    }

    public int libraryId()
    {
        return libraryId;
    }

    public String toString()
    {
        return "LibraryInfo{" +
            "acceptor=" + acceptor +
            ", libraryId=" + libraryId +
            '}';
    }
}
