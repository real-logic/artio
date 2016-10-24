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
package uk.co.real_logic.fix_gateway.validation;

import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;

import static uk.co.real_logic.fix_gateway.validation.PersistenceLevel.LOCAL_ARCHIVE;
import static uk.co.real_logic.fix_gateway.validation.PersistenceLevel.REPLICATED;

/**
 * Determines whether a session should be replicated or not.
 */
@FunctionalInterface
public interface SessionPersistenceStrategy
{
    static SessionPersistenceStrategy alwaysReplicated()
    {
        return (logon) -> REPLICATED;
    }

    static SessionPersistenceStrategy alwaysLocallyArchive()
    {
        return (logon) -> LOCAL_ARCHIVE;
    }

    static boolean resetSequenceNumbersUponLogon(final PersistenceLevel persistenceLevel)
    {
        switch (persistenceLevel)
        {
            case REPLICATED:
                return false;
            case LOCAL_ARCHIVE:
                return true;
            default:
                throw new IllegalArgumentException("persistenceLevel=" + persistenceLevel);
        }
    }

    PersistenceLevel getPersistenceLevel(LogonDecoder logon);
}
