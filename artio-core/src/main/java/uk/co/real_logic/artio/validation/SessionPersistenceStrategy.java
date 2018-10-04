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
package uk.co.real_logic.artio.validation;

import uk.co.real_logic.artio.decoder.LogonDecoder;

import static uk.co.real_logic.artio.validation.PersistenceLevel.UNINDEXED;
import static uk.co.real_logic.artio.validation.PersistenceLevel.INDEXED;

/**
 * Determines whether a session should be replicated or not.
 */
@FunctionalInterface
public interface SessionPersistenceStrategy
{
    static SessionPersistenceStrategy alwaysReplicated()
    {
        return (logon) -> INDEXED;
    }

    static SessionPersistenceStrategy alwaysLocallyArchive()
    {
        return (logon) -> UNINDEXED;
    }

    static boolean resetSequenceNumbersUponLogon(final PersistenceLevel persistenceLevel)
    {
        switch (persistenceLevel)
        {
            case INDEXED:
                return false;
            case UNINDEXED:
                return true;
            default:
                throw new IllegalArgumentException("persistenceLevel=" + persistenceLevel);
        }
    }

    PersistenceLevel getPersistenceLevel(LogonDecoder logon);
}
