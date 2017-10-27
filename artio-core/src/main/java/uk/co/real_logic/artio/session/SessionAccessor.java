/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.session;

import uk.co.real_logic.artio.library.FixLibrary;

/**
 * Encapsulate methods on the Session Object from the outside world whilst allowing them to be
 * called from the library itself.
 */
public class SessionAccessor
{
    public SessionAccessor(final Class<?> witness)
    {
        if (isNotInsideLibraryPackage(witness))
        {
            throw new IllegalStateException("Must be instantiated by the library");
        }
    }

    private boolean isNotInsideLibraryPackage(final Class<?> witness)
    {
        return FixLibrary.class.getPackage() != witness.getPackage() ||
            !"LibraryPoller".equals(witness.getSimpleName());
    }

    public void libraryConnected(final Session session, final boolean libraryConnected)
    {
        session.libraryConnected(libraryConnected);
    }

    public void disable(final Session session)
    {
        session.disable();
    }
}
