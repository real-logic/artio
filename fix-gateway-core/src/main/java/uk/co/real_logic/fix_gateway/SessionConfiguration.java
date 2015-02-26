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
package uk.co.real_logic.fix_gateway;

import java.util.Objects;

/**
 * Immutable Configuration class.
 */
public final class SessionConfiguration
{
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    public static Builder builder()
    {
        return new Builder();
    }

    private SessionConfiguration(final String host, final int port, final String username, final String password)
    {
        Objects.requireNonNull(host);
        Objects.requireNonNull(username);
        Objects.requireNonNull(password);

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    // TODO: add a timezone configuration option

    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    public String username()
    {
        return username;
    }

    public String password()
    {
        return password;
    }

    public static final class Builder
    {
        private String username;
        private String password;
        private String host;
        private int port;

        private Builder()
        {
        }

        public Builder credentials(final String username, final String password)
        {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder address(final String host, final int port)
        {
            this.host = host;
            this.port = port;
            return this;
        }

        public SessionConfiguration build()
        {
            return new SessionConfiguration(host, port, username, password);
        }
    }

}
