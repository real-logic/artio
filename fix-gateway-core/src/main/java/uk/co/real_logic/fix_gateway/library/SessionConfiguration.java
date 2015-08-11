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
package uk.co.real_logic.fix_gateway.library;

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
    private final String senderCompId;
    private final String targetCompId;
    private final String senderSubId;
    private final String senderLocationId;

    public static Builder builder()
    {
        return new Builder();
    }

    private SessionConfiguration(
        final String host, final int port, final String username, final String password, final String senderCompId,
        final String senderSubId, final String senderLocationId, final String targetCompId)
    {
        Objects.requireNonNull(host);
        Objects.requireNonNull(senderCompId);
        Objects.requireNonNull(senderSubId);
        Objects.requireNonNull(senderLocationId);
        Objects.requireNonNull(targetCompId);

        this.senderCompId = senderCompId;
        this.senderSubId = senderSubId;
        this.senderLocationId = senderLocationId;
        this.targetCompId = targetCompId;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

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

    public String senderCompId()
    {
        return senderCompId;
    }

    public String senderSubId()
    {
        return senderSubId;
    }

    public String senderLocationId()
    {
        return senderLocationId;
    }

    public String targetCompId()
    {
        return targetCompId;
    }

    public static final class Builder
    {
        private String username;
        private String password;
        private String host;
        private int port;
        private String senderCompId;
        private String targetCompId;
        private String senderSubId = "";
        private String senderLocationId = "";

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

        public Builder senderCompId(final String senderCompId)
        {
            this.senderCompId = senderCompId;
            return this;
        }

        public Builder senderSubId(final String senderSubId)
        {
            this.senderSubId = senderSubId;
            return this;
        }

        public Builder senderLocationId(final String senderLocationId)
        {
            this.senderLocationId = senderLocationId;
            return this;
        }

        public Builder targetCompId(final String targetCompId)
        {
            this.targetCompId = targetCompId;
            return this;
        }

        public SessionConfiguration build()
        {
            return new SessionConfiguration(host, port, username, password, senderCompId, senderSubId,
                senderLocationId, targetCompId);
        }
    }

}
