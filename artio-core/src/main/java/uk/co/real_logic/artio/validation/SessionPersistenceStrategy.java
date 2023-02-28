/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.validation;

import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;

import static uk.co.real_logic.artio.validation.PersistenceLevel.PERSISTENT_SEQUENCE_NUMBERS;
import static uk.co.real_logic.artio.validation.PersistenceLevel.TRANSIENT_SEQUENCE_NUMBERS;

/**
 * Determines whether a session should reset it's sequence numbers on logon or not.
 */
@FunctionalInterface
public interface SessionPersistenceStrategy
{
    /**
     * Deprecated and will be removed in future. Renamed to {@link #alwaysPersistent}.
     *
     * @return {@link #alwaysPersistent}
     */
    @Deprecated
    static SessionPersistenceStrategy alwaysIndexed()
    {
        return alwaysPersistent();
    }

    /**
     * Deprecated and will be removed in future. Renamed to {@link #alwaysTransient}.
     *
     * @return {@link #alwaysPersistent}
     */
    @Deprecated
    static SessionPersistenceStrategy alwaysUnindexed()
    {
        return alwaysTransient();
    }

    static SessionPersistenceStrategy alwaysPersistent()
    {
        return (logon) -> PERSISTENT_SEQUENCE_NUMBERS;
    }

    static SessionPersistenceStrategy alwaysTransient()
    {
        return (logon) -> TRANSIENT_SEQUENCE_NUMBERS;
    }

    static boolean resetSequenceNumbersUponLogon(final PersistenceLevel persistenceLevel)
    {
        switch (persistenceLevel)
        {
            case PERSISTENT_SEQUENCE_NUMBERS:
            case INDEXED:
                return false;

            case TRANSIENT_SEQUENCE_NUMBERS:
            case UNINDEXED:
                return true;

            default:
                throw new IllegalArgumentException("persistenceLevel=" + persistenceLevel);
        }
    }

    PersistenceLevel getPersistenceLevel(AbstractLogonDecoder logon);
}
