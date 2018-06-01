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
package uk.co.real_logic.artio.library;

import org.agrona.collections.IntArrayList;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.messages.SequenceNumberType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_REPLY_TIMEOUT_IN_MS;

/**
 * Immutable Configuration class for a single initiated session.
 */
public final class SessionConfiguration
{
    public static final int AUTOMATIC_INITIAL_SEQUENCE_NUMBER = -1;
    public static final boolean DEFAULT_RESET_SEQ_NUM = false;
    public static final boolean DEFAULT_SEQUENCE_NUMBERS_PERSISTENT = false;

    private final List<String> hosts;
    private final IntArrayList ports;
    private final String username;
    private final String password;
    private final String senderCompId;
    private final String senderSubId;
    private final String senderLocationId;
    private final String targetCompId;
    private final String targetSubId;
    private final String targetLocationId;
    private final boolean sequenceNumbersPersistent;
    private final int initialReceivedSequenceNumber;
    private final int initialSentSequenceNumber;
    private final long timeoutInMs;
    private final boolean resetSeqNum;

    public static Builder builder()
    {
        return new Builder();
    }

    private SessionConfiguration(
        final List<String> hosts,
        final IntArrayList ports,
        final String username,
        final String password,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String targetSubId,
        final String targetLocationId,
        final boolean sequenceNumbersPersistent,
        final int initialReceivedSequenceNumber,
        final int initialSentSequenceNumber,
        final long timeoutInMs,
        final boolean resetSeqNum)
    {
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(ports);
        Objects.requireNonNull(senderCompId);
        Objects.requireNonNull(senderSubId);
        Objects.requireNonNull(senderLocationId);
        Objects.requireNonNull(targetCompId);
        Objects.requireNonNull(targetSubId);
        Objects.requireNonNull(targetLocationId);

        requireNonEmpty(hosts, "hosts");
        requireNonEmpty(ports, "ports");

        this.senderCompId = senderCompId;
        this.senderSubId = senderSubId;
        this.senderLocationId = senderLocationId;
        this.targetCompId = targetCompId;
        this.targetSubId = targetSubId;
        this.targetLocationId = targetLocationId;
        this.timeoutInMs = timeoutInMs;
        this.hosts = hosts;
        this.ports = ports;
        this.username = username;
        this.password = password;
        this.sequenceNumbersPersistent = sequenceNumbersPersistent;
        this.initialReceivedSequenceNumber = initialReceivedSequenceNumber;
        this.initialSentSequenceNumber = initialSentSequenceNumber;
        this.resetSeqNum = resetSeqNum;
    }

    private void requireNonEmpty(final List<?> values, final String name)
    {
        if (values.isEmpty())
        {
            throw new IllegalArgumentException(name + " is empty");
        }
    }

    public List<String> hosts()
    {
        return hosts;
    }

    public IntArrayList ports()
    {
        return ports;
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

    public String targetSubId()
    {
        return targetSubId;
    }

    public String targetLocationId()
    {
        return targetLocationId;
    }

    public boolean sequenceNumbersPersistent()
    {
        return sequenceNumbersPersistent;
    }

    public SequenceNumberType sequenceNumberType()
    {
        return sequenceNumbersPersistent ? SequenceNumberType.PERSISTENT : SequenceNumberType.TRANSIENT;
    }

    public int initialReceivedSequenceNumber()
    {
        return initialReceivedSequenceNumber;
    }

    public int initialSentSequenceNumber()
    {
        return initialSentSequenceNumber;
    }

    public long timeoutInMs()
    {
        return timeoutInMs;
    }

    public boolean resetSeqNum()
    {
        return resetSeqNum;
    }

    @Override
    public String toString()
    {
        return "SessionConfiguration{" +
            "hosts=" + hosts +
            ", ports=" + ports +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", senderCompId='" + senderCompId + '\'' +
            ", senderSubId='" + senderSubId + '\'' +
            ", senderLocationId='" + senderLocationId + '\'' +
            ", targetCompId='" + targetCompId + '\'' +
            ", targetSubId='" + targetSubId + '\'' +
            ", targetLocationId='" + targetLocationId + '\'' +
            ", sequenceNumbersPersistent=" + sequenceNumbersPersistent +
            ", initialReceivedSequenceNumber=" + initialReceivedSequenceNumber +
            ", initialSentSequenceNumber=" + initialSentSequenceNumber +
            ", timeoutInMs=" + timeoutInMs +
            ", resetSeqNum=" + resetSeqNum +
            '}';
    }

    public static final class Builder
    {
        private String username;
        private String password;
        private List<String> hosts = new ArrayList<>();
        private IntArrayList ports = new IntArrayList();
        private String senderCompId;
        private String senderSubId = "";
        private String senderLocationId = "";
        private String targetCompId;
        private String targetSubId = "";
        private String targetLocationId = "";
        private boolean sequenceNumbersPersistent = DEFAULT_SEQUENCE_NUMBERS_PERSISTENT;
        private int initialReceivedSequenceNumber = AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
        private int initialSentSequenceNumber = AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
        private long timeoutInMs = DEFAULT_REPLY_TIMEOUT_IN_MS;
        private boolean resetSeqNum = DEFAULT_RESET_SEQ_NUM;

        private Builder()
        {
        }

        /**
         * Sets the authentication credentials to use the FIX session's logon.
         * <p>
         * Optional
         *
         * @param username the username to use in logon messages.
         * @param password the password to use in logon messages.
         * @return this
         */
        public Builder credentials(final String username, final String password)
        {
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Sets the remote address to connect to. This can be called multiple times and each will be tried
         * and round-robin'd.
         *
         * @param host the hostname to connect to.
         * @param port the port to connect to.
         * @return this
         */
        public Builder address(final String host, final int port)
        {
            hosts.add(host);
            ports.addInt(port);
            return this;
        }

        /**
         * Sets the sender company id used by messages in this session.
         *
         * @param senderCompId the sender company id.
         * @return this
         */
        public Builder senderCompId(final String senderCompId)
        {
            this.senderCompId = senderCompId;
            return this;
        }

        /**
         * Sets the sender sub company id used by messages in this session.
         * <p>
         * Optional
         *
         * @param senderSubId the sender sub company id.
         * @return this
         */
        public Builder senderSubId(final String senderSubId)
        {
            this.senderSubId = senderSubId;
            return this;
        }

        /**
         * Sets the sender location company id used by messages in this session.
         * <p>
         * Optional
         *
         * @param senderLocationId the sender location company id.
         * @return this
         */
        public Builder senderLocationId(final String senderLocationId)
        {
            this.senderLocationId = senderLocationId;
            return this;
        }

        /**
         * Sets the target company id used by messages in this session.
         *
         * @param targetCompId the target company id.
         * @return this
         */
        public Builder targetCompId(final String targetCompId)
        {
            this.targetCompId = targetCompId;
            return this;
        }

        public Builder targetSubId(final String targetSubId)
        {
            this.targetSubId = targetSubId;
            return this;
        }

        public Builder targetLocationId(final String targetLocationId)
        {
            this.targetLocationId = targetLocationId;
            return this;
        }

        /**
         * Set this flag if you want sequence numbers to persistent when you reconnect
         * to the acceptor.
         *
         * @param sequenceNumbersPersistent true to make sequence numbers persistent
         * @return this builder
         *
         * @see this#initialReceivedSequenceNumber(int)
         * @see this#initialSentSequenceNumber(int)
         */
        public Builder sequenceNumbersPersistent(final boolean sequenceNumbersPersistent)
        {
            this.sequenceNumbersPersistent = sequenceNumbersPersistent;
            return this;
        }

        /**
         * Sets the initial sequence number that you expect from use an acceptor when connecting to it.
         *
         * @param initialReceivedSequenceNumber the msg sequence number to expect from their logon message.
         * @return this builder
         *
         * @see this#sequenceNumbersPersistent(boolean)
         * @see this#initialSentSequenceNumber(int)
         */
        public Builder initialReceivedSequenceNumber(final int initialReceivedSequenceNumber)
        {
            this.initialReceivedSequenceNumber = initialReceivedSequenceNumber;
            return this;
        }

        /**
         * Sets the initial sequence number that you use for your logon message when connecting to an acceptor.
         *
         * @param initialSentSequenceNumber the msg sequence number to use when you send your logon message.
         * @return this builder
         *
         * @see this#sequenceNumbersPersistent(boolean)
         * @see this#initialReceivedSequenceNumber(int)
         */
        public Builder initialSentSequenceNumber(final int initialSentSequenceNumber)
        {
            this.initialSentSequenceNumber = initialSentSequenceNumber;
            return this;
        }

        /**
         * Sets the timeout for this operation in milliseconds. Note that this includes both the time to
         * communicate with the engine and also to perform the initiation of the TCP connection and logon
         * to the external system.
         *
         * @param timeoutInMs the timeout for this operation
         * @return this builder
         */
        public Builder timeoutInMs(final long timeoutInMs)
        {
            CommonConfiguration.validateTimeout(timeoutInMs);
            this.timeoutInMs = timeoutInMs;
            return this;
        }

        /**
         * Sets the value of the resetSeqNum (141=) flag when the initiator logon message is sent.
         *
         * @param resetSeqNum the value of the resetSeqNum (141=) flag when the initiator logon message is sent.
         * @return this builder
         */
        public Builder resetSeqNum(final boolean resetSeqNum)
        {
            this.resetSeqNum = resetSeqNum;
            return this;
        }

        public SessionConfiguration build()
        {
            return new SessionConfiguration(
                hosts,
                ports,
                username,
                password,
                senderCompId,
                senderSubId,
                senderLocationId,
                targetCompId,
                targetSubId,
                targetLocationId,
                sequenceNumbersPersistent,
                initialReceivedSequenceNumber,
                initialSentSequenceNumber,
                timeoutInMs,
                resetSeqNum);
        }
    }
}
