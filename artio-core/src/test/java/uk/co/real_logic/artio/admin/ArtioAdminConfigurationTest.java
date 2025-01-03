/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.admin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static uk.co.real_logic.artio.admin.ArtioAdminConfiguration.CONNECT_TIMEOUT_PROP;

class ArtioAdminConfigurationTest
{
    @Test
    void connectTimeoutDefaultsToFiveSeconds()
    {
        final ArtioAdminConfiguration configuration = new ArtioAdminConfiguration();
        assertEquals(TimeUnit.SECONDS.toNanos(5), configuration.connectTimeoutNs());
    }

    @ParameterizedTest
    @CsvSource({ "1s,1000000000", "123,123", "4ns,4", "45us,45000", "3ms,3000000" })
    void connectTimeoutCanBeConfiguredViaSystemProperty(final String propValue, final long expectedValueNs)
    {
        System.setProperty(CONNECT_TIMEOUT_PROP, propValue);
        try
        {
            final ArtioAdminConfiguration configuration = new ArtioAdminConfiguration();
            assertEquals(expectedValueNs, configuration.connectTimeoutNs());
        }
        finally
        {
            System.clearProperty(CONNECT_TIMEOUT_PROP);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { Long.MIN_VALUE, Long.MAX_VALUE, 0, 122331231231L, 7767 })
    void connectTimeoutCanBeSetExplicitly(final long value)
    {
        final ArtioAdminConfiguration configuration = new ArtioAdminConfiguration();
        configuration.connectTimeoutNs(value);

        assertEquals(value, configuration.connectTimeoutNs());
    }

    @Test
    void connectTimeoutCannotBeNegative()
    {
        final ArtioAdminConfiguration configuration = new ArtioAdminConfiguration();
        configuration.connectTimeoutNs(-1);

        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, configuration::conclude);
        assertEquals("connectTimeoutNs cannot be negative: -1", exception.getMessage());
    }
}
