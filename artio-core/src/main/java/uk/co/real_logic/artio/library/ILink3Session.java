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

import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.ilink.AbstractILink3Offsets;
import uk.co.real_logic.artio.ilink.AbstractILink3Proxy;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.artio.ilink.AbstractILink3Offsets.MISSING_OFFSET;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.DisconnectReason.FAILED_AUTHENTICATION;
import static uk.co.real_logic.artio.messages.DisconnectReason.LOGOUT;

// NB: This is an experimental API and is subject to change or potentially removal.
public class ILink3Session
{
    public static final long MICROS_IN_MILLIS = 1_000;
    public static final long NANOS_IN_MICROS = 1_000;
    private static final long NANOS_IN_MILLIS = MICROS_IN_MILLIS * NANOS_IN_MICROS;

    public enum State
    {
        /** TCP connection established, negotiate not sent.*/
        CONNECTED,
        /** Negotiate sent but no reply received */
        SENT_NEGOTIATE,
        RETRY_NEGOTIATE,

        NEGOTIATE_REJECTED,
        /** Negotiate accepted, Establish not sent */
        NEGOTIATED,
        /** Negotiate accepted, Establish sent */
        SENT_ESTABLISH,
        RETRY_ESTABLISH,
        ESTABLISH_REJECTED,
        /** Establish accepted, messages can be exchanged */
        ESTABLISHED,
        UNBINDING,
        SENT_TERMINATE,
        UNBOUND
    }

    private final AbstractILink3Proxy proxy;
    private final AbstractILink3Offsets offsets;
    private final ILink3SessionConfiguration configuration;
    private final long connectionId;
    private InitiateILink3SessionReply initiateReply;
    private final GatewayPublication outboundPublication;
    private final int libraryId;
    private final LibraryPoller owner;

    private final long uuid;
    private State state;
    private int nextSentSeqNo;

    private long resendTime;

    ILink3Session(
        final AbstractILink3Proxy proxy,
        final AbstractILink3Offsets offsets,
        final ILink3SessionConfiguration configuration,
        final long connectionId,
        final InitiateILink3SessionReply initiateReply,
        final GatewayPublication outboundPublication,
        final int libraryId,
        final LibraryPoller owner,
        final long uuid,
        final int lastReceivedSequenceNumber,
        final int lastSentSequenceNumber)
    {
        this.proxy = proxy;
        this.offsets = offsets;
        this.configuration = configuration;
        this.connectionId = connectionId;
        this.initiateReply = initiateReply;
        this.outboundPublication = outboundPublication;
        this.libraryId = libraryId;
        this.owner = owner;

        nextSentSeqNo = calculateInitialSentSequenceNumber(configuration, lastSentSequenceNumber);
        state = State.CONNECTED;
        this.uuid = uuid;

        if (configuration.reestablishLastSession())
        {
            sendEstablish();
        }
        else
        {
            sendNegotiate();
        }
    }

    // PUBLIC API

    public long claimMessage(
        final MessageEncoderFlyweight message)
    {
        final long position = proxy.claimILinkMessage(message.sbeBlockLength(), message);

        if (position > 0)
        {
            final int templateId = message.sbeTemplateId();
            final MutableDirectBuffer buffer = message.buffer();
            final int messageOffset = message.offset();

            final int seqNumOffset = offsets.seqNumOffset(templateId);
            if (seqNumOffset != MISSING_OFFSET)
            {
                buffer.putInt(messageOffset + seqNumOffset, nextSentSeqNo++, LITTLE_ENDIAN);
            }

            // NB: possRetrans field does not need to be set because it is always false in this claim API
            // and the false byte is 0, which is what Aeron buffers are initialised to.
        }

        return position;
    }

    public void commit()
    {
        proxy.commit();
    }

    public long terminate(final String reason, final int errorCodes)
    {
        validateCanSend();

        final long position = sendTerminate(reason, errorCodes);

        if (position > 0)
        {
            state = State.UNBINDING;
        }

        return position;
    }

    private long sendTerminate(final String reason, final int errorCodes)
    {
        final long requestTimestamp = requestTimestamp();
        return proxy.sendTerminate(
            reason,
            uuid,
            requestTimestamp,
            errorCodes);
    }

    private void validateCanSend()
    {
        if (state != State.ESTABLISHED)
        {
            throw new IllegalStateException("State should be ESTABLISHED in order to send but is " + state);
        }
    }

    public long requestDisconnect(final DisconnectReason reason)
    {
        return outboundPublication.saveRequestDisconnect(libraryId, connectionId, reason);
    }

    public long uuid()
    {
        return uuid;
    }

    public long connectionId()
    {
        return connectionId;
    }

    public State state()
    {
        return state;
    }

    // END PUBLIC API

    private int calculateInitialSentSequenceNumber(
        final ILink3SessionConfiguration configuration, final int lastSentSequenceNumber)
    {
        if (!configuration.reestablishLastSession())
        {
            return 1;
        }

        final int initialSentSequenceNumber = configuration.initialSentSequenceNumber();
        if (initialSentSequenceNumber == AUTOMATIC_INITIAL_SEQUENCE_NUMBER)
        {
            return lastSentSequenceNumber + 1;
        }
        return initialSentSequenceNumber;
    }

    private long requestTimestamp()
    {
        final long nanoseconds = System.nanoTime() % NANOS_IN_MILLIS;
        return System.currentTimeMillis() * NANOS_IN_MILLIS + nanoseconds;
    }

    private boolean sendNegotiate()
    {
        final long requestTimestamp = requestTimestamp();
        final String sessionId = configuration.sessionId();
        final String firmId = configuration.firmId();
        final String canonicalMsg = String.valueOf(requestTimestamp) + '\n' + uuid + '\n' + sessionId + '\n' + firmId;
        final byte[] hMACSignature = calculateHMAC(canonicalMsg);

        final long position = proxy.sendNegotiate(
            hMACSignature, configuration.accessKeyId(), uuid, requestTimestamp, sessionId, firmId);

        if (position > 0)
        {
            state = State.SENT_NEGOTIATE;
            resendTime = nextTimeout();
            return true;
        }

        return false;
    }

    private boolean sendEstablish()
    {
        final long requestTimestamp = requestTimestamp();
        final String sessionId = configuration.sessionId();
        final String firmId = configuration.firmId();
        final String tradingSystemName = configuration.tradingSystemName();
        final String tradingSystemVersion = configuration.tradingSystemVersion();
        final String tradingSystemVendor = configuration.tradingSystemVendor();
        final int keepAliveInterval = configuration.requestedKeepAliveIntervalInMs();
        final String accessKeyId = configuration.accessKeyId();

        final String canonicalMsg = String.valueOf(requestTimestamp) + '\n' + uuid + '\n' + sessionId +
            '\n' + firmId + '\n' + tradingSystemName + '\n' + tradingSystemVersion + '\n' + tradingSystemVendor +
            '\n' + nextSentSeqNo + '\n' + keepAliveInterval;
        final byte[] hMACSignature = calculateHMAC(canonicalMsg);

        final long position = proxy.sendEstablish(hMACSignature,
            accessKeyId,
            tradingSystemName,
            tradingSystemVendor,
            tradingSystemVersion,
            uuid,
            requestTimestamp,
            nextSentSeqNo,
            sessionId,
            firmId,
            keepAliveInterval);

        if (position > 0)
        {
            resendTime = nextTimeout();
            state = State.SENT_ESTABLISH;
            return true;
        }

        return false;
    }

    private long nextTimeout()
    {
        return System.currentTimeMillis() + configuration.requestedKeepAliveIntervalInMs();
    }

    private byte[] calculateHMAC(final String canonicalRequest)
    {
        final String userKey = configuration.userKey();

        try
        {
            final Mac sha256HMAC = getHmac();

            // Decode the key first, since it is base64url encoded
            final byte[] decodedUserKey = Base64.getUrlDecoder().decode(userKey);
            final SecretKeySpec secretKey = new SecretKeySpec(decodedUserKey, "HmacSHA256");
            sha256HMAC.init(secretKey);

            // Calculate HMAC
            return sha256HMAC.doFinal(canonicalRequest.getBytes(UTF_8));
        }
        catch (final InvalidKeyException | IllegalStateException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    private Mac getHmac()
    {
        try
        {
            return Mac.getInstance("HmacSHA256");
        }
        catch (final NoSuchAlgorithmException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    int poll(final long timeInMs)
    {
        final State state = this.state;
        switch (state)
        {
            case CONNECTED:
                return sendNegotiate() ? 1 : 0;

            case SENT_NEGOTIATE:
                if (timeInMs > resendTime)
                {
                    if (sendNegotiate())
                    {
                        this.state = State.RETRY_NEGOTIATE;
                        return 1;
                    }
                }
                break;

            case RETRY_NEGOTIATE:
                if (timeInMs > resendTime)
                {
                    initiateReply.onNegotiateFailure();
                    fullyUnbind();
                    return 1;
                }
                break;

            case NEGOTIATED:
                return sendEstablish() ? 1 : 0;

            case RETRY_ESTABLISH:
                if (timeInMs > resendTime)
                {
                    initiateReply.onEstablishFailure();
                    fullyUnbind();
                    return 1;
                }
                break;

            case SENT_ESTABLISH:
                if (timeInMs > resendTime)
                {
                    if (sendEstablish())
                    {
                        this.state = State.RETRY_ESTABLISH;
                        return 1;
                    }
                }
                break;
        }
        return 0;
    }

    // EVENT HANDLERS

    long onNegotiationResponse(
        final long uUID,
        final long requestTimestamp,
        final int secretKeySecureIDExpiration,
        final long previousSeqNo,
        final long previousUUID)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // TODO: validate request timestamp
        // TODO: calculate session expiration
        // TODO: check gap with previous sequence number and uuid

        state = State.NEGOTIATED;
        sendEstablish();

        return 1; // TODO: move to action
    }

    long onNegotiationReject(
        final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // TODO: validate request timestamp
        state = State.NEGOTIATE_REJECTED;
        connectionError(new RuntimeException("Negotiate rejected: " + reason + ",errorCodes=" + errorCodes));

        return 1;
    }

    private void connectionError(final Exception error)
    {
        initiateReply.onError(error);
        initiateReply = null;
        requestDisconnect(FAILED_AUTHENTICATION);
        owner.onUnbind(this);
    }

    long onEstablishmentAck(
        final long uUID,
        final long requestTimestamp,
        final long nextSeqNo,
        final long previousSeqNo,
        final long previousUUID,
        final int keepAliveInterval,
        final int secretKeySecureIDExpiration)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // TODO: validate request timestamp
        // TODO: calculate session expiration
        // TODO: check gap with previous sequence number and uuid

        state = State.ESTABLISHED;
        initiateReply.onComplete(this);
        initiateReply = null;

        return 1;
    }

    long onEstablishmentReject(
        final String reason, final long uUID, final long requestTimestamp, final long nextSeqNo, final int errorCodes)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // TODO: validate request timestamp

        state = State.ESTABLISH_REJECTED;
        connectionError(new RuntimeException(
            "Establishment rejected: " + reason + ",nextSeqNo=" + nextSeqNo + ",errorCodes=" + errorCodes));

        return 1;
    }

    long onTerminate(final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // TODO: validate request timestamp

        // We initiated termination
        if (state == State.UNBINDING)
        {
            fullyUnbind();
        }
        // The exchange initiated termination
        else
        {
            // TODO: handle backpressure properly
            sendTerminate(reason, errorCodes);
            fullyUnbind();
        }

        return 1;
    }

    private void fullyUnbind()
    {
        // TODO: linger state = State.SENT_TERMINATE;
        state = State.UNBOUND;
        requestDisconnect(LOGOUT);
        owner.onUnbind(this);
    }

}
