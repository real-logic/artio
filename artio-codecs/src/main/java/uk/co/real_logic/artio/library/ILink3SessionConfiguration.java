/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.library;

import org.agrona.Verify;
import uk.co.real_logic.artio.ilink.ILink3SessionHandler;

// NB: This is an experimental API and is subject to change or potentially removal.
public class ILink3SessionConfiguration
{
    public static final int DEFAULT_REQUESTED_KEEP_ALIVE_INTERVAL = 10_000;
    public static final long AUTOMATIC_INITIAL_SEQUENCE_NUMBER = -1L;


    private String host;
    private int port;
    private String sessionId;
    private String firmId;
    private String tradingSystemName = "";
    private String tradingSystemVersion = "";
    private String tradingSystemVendor = "";
    private int requestedKeepAliveIntervalInMs = DEFAULT_REQUESTED_KEEP_ALIVE_INTERVAL;
    private String userKey;
    private long initialSentSequenceNumber = AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
    private long initialReceivedSequenceNumber = AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
    private String accessKeyId;
    private boolean reEstablishLastSession = false;
    private ILink3SessionHandler handler;

    public ILink3SessionConfiguration host(final String host)
    {
        this.host = host;
        return this;
    }

    public String host()
    {
        return host;
    }

    public ILink3SessionConfiguration port(final int port)
    {
        this.port = port;
        return this;
    }

    public int port()
    {
        return port;
    }

    public ILink3SessionConfiguration sessionId(final String sessionId)
    {
        this.sessionId = sessionId;
        return this;
    }

    public String sessionId()
    {
        return sessionId;
    }

    public ILink3SessionConfiguration firmId(final String firmId)
    {
        this.firmId = firmId;
        return this;
    }

    public String firmId()
    {
        return firmId;
    }

    public ILink3SessionConfiguration tradingSystemName(final String tradingSystemName)
    {
        this.tradingSystemName = tradingSystemName;
        return this;
    }

    public String tradingSystemName()
    {
        return tradingSystemName;
    }

    public ILink3SessionConfiguration tradingSystemVersion(final String tradingSystemVersion)
    {
        this.tradingSystemVersion = tradingSystemVersion;
        return this;
    }

    public String tradingSystemVersion()
    {
        return tradingSystemVersion;
    }

    public ILink3SessionConfiguration tradingSystemVendor(final String tradingSystemVendor)
    {
        this.tradingSystemVendor = tradingSystemVendor;
        return this;
    }

    public String tradingSystemVendor()
    {
        return tradingSystemVendor;
    }

    public ILink3SessionConfiguration requestedKeepAliveIntervalInMs(final int requestedKeepAliveIntervalInMs)
    {
        this.requestedKeepAliveIntervalInMs = requestedKeepAliveIntervalInMs;
        return this;
    }

    public int requestedKeepAliveIntervalInMs()
    {
        return requestedKeepAliveIntervalInMs;
    }

    public ILink3SessionConfiguration userKey(final String userKey)
    {
        this.userKey = userKey;
        return this;
    }

    public String userKey()
    {
        return userKey;
    }

    public ILink3SessionConfiguration initialSentSequenceNumber(final long initialSentSequenceNumber)
    {
        this.initialSentSequenceNumber = initialSentSequenceNumber;
        return this;
    }

    public long initialSentSequenceNumber()
    {
        return initialSentSequenceNumber;
    }

    public ILink3SessionConfiguration initialReceivedSequenceNumber(final long initialReceivedSequenceNumber)
    {
        this.initialReceivedSequenceNumber = initialReceivedSequenceNumber;
        return this;
    }

    public long initialReceivedSequenceNumber()
    {
        return initialReceivedSequenceNumber;
    }

    public ILink3SessionConfiguration accessKeyId(final String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public String accessKeyId()
    {
        return accessKeyId;
    }

    /**
     * Enable a re-establishment of the same session with the same UUID, rather than generating a new UUID.
     * If there is an existing UUID associated with this session identifier then that will be used. The
     * session identifier here is a triple of (port, host and accessKeyId).
     *
     * Note: if this session has never connected before then a new UUID will be generated.
     *
     * @param reEstablishLastSession true to re-establish the session, false otherwise.
     * @return this.
     */
    public ILink3SessionConfiguration reEstablishLastSession(final boolean reEstablishLastSession)
    {
        this.reEstablishLastSession = reEstablishLastSession;
        return this;
    }

    public boolean reEstablishLastSession()
    {
        return reEstablishLastSession;
    }

    public ILink3SessionConfiguration handler(final ILink3SessionHandler handler)
    {
        this.handler = handler;
        return this;
    }

    public ILink3SessionHandler handler()
    {
        return handler;
    }

    public void validate()
    {
        Verify.notNull(host, "host");
        Verify.notNull(sessionId, "sessionId");
        Verify.notNull(firmId, "firmId");
        Verify.notNull(userKey, "userKey");
        Verify.notNull(accessKeyId, "accessKeyId");
        Verify.notNull(handler, "handler");

        if (requestedKeepAliveIntervalInMs <= 0)
        {
            throw new IllegalArgumentException("requestedKeepAliveInterval must be positive, but is: " +
                requestedKeepAliveIntervalInMs);
        }

        if (port <= 0)
        {
            throw new IllegalArgumentException("port must be positive, but is: " + port);
        }
    }

    public String toString()
    {
        return "ILink3SessionConfiguration{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", sessionId='" + sessionId + '\'' +
            ", firmId='" + firmId + '\'' +
            ", tradingSystemName='" + tradingSystemName + '\'' +
            ", tradingSystemVersion='" + tradingSystemVersion + '\'' +
            ", tradingSystemVendor='" + tradingSystemVendor + '\'' +
            ", keepAliveInterval=" + requestedKeepAliveIntervalInMs +
            ", userKey='" + userKey + '\'' +
            ", initialSentSequenceNumber=" + initialSentSequenceNumber +
            ", accessKeyId=" + accessKeyId +
            ", handler=" + handler +
            '}';
    }

    public int retransmitRequestMessageLimit()
    {
        return 2500;
    }
}
