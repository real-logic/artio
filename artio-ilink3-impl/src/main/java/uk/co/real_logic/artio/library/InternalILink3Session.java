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

import iLinkBinary.FTI;
import iLinkBinary.KeepAliveLapsed;
import io.aeron.exceptions.TimeoutException;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.ilink.*;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.TimeUtil;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static iLinkBinary.KeepAliveLapsed.Lapsed;
import static iLinkBinary.KeepAliveLapsed.NotLapsed;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.artio.ilink.AbstractILink3Offsets.MISSING_OFFSET;
import static uk.co.real_logic.artio.ilink.AbstractILink3Parser.BOOLEAN_FLAG_TRUE;
import static uk.co.real_logic.artio.library.ILink3SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.DisconnectReason.FAILED_AUTHENTICATION;
import static uk.co.real_logic.artio.messages.DisconnectReason.LOGOUT;
import static uk.co.real_logic.artio.util.TimeUtil.nanoSecondTimestamp;

/**
 * External users should never rely on this API.
 */
public class InternalILink3Session extends ILink3Session
{
    private static final UnsafeBuffer NO_BUFFER = new UnsafeBuffer();

    private final NotAppliedResponse response = new NotAppliedResponse();
    private final Deque<RetransmitRequest> retransmitRequests = new ArrayDeque<>();

    private final ILink3Proxy proxy;
    private final ILink3Offsets offsets;
    private final ILink3SessionConfiguration configuration;
    private final long connectionId;
    private final GatewayPublication outboundPublication;
    private final GatewayPublication inboundPublication;
    private final int libraryId;
    private final LibraryPoller owner;
    private final ILink3SessionHandler handler;
    private final long uuid;

    private InitiateILink3SessionReply initiateReply;

    private State state;
    private int nextRecvSeqNo;
    private int nextSentSeqNo;
    private long retransmitFillSeqNo = NOT_AWAITING_RETRANSMIT;

    private long resendTime;
    private long nextReceiveMessageTimeInMs;
    private long nextSendMessageTimeInMs;

    public InternalILink3Session(
        final ILink3SessionConfiguration configuration,
        final long connectionId,
        final InitiateILink3SessionReply initiateReply,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final LibraryPoller owner,
        final long uuid,
        final int lastReceivedSequenceNumber,
        final int lastSentSequenceNumber)
    {
        this.configuration = configuration;
        this.connectionId = connectionId;
        this.initiateReply = initiateReply;
        this.outboundPublication = outboundPublication;
        this.inboundPublication = inboundPublication;
        this.libraryId = libraryId;
        this.owner = owner;
        this.handler = configuration.handler();

        proxy = new ILink3Proxy(connectionId, outboundPublication.dataPublication());
        offsets = new ILink3Offsets();
        nextSentSeqNo = calculateInitialSentSequenceNumber(configuration, lastSentSequenceNumber);
        nextRecvSeqNo = calculateInitialSentSequenceNumber(configuration, lastReceivedSequenceNumber);
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
        validateCanSend();

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

            final int sendingTimeEpochOffset = offsets.sendingTimeEpochOffset(templateId);
            if (sendingTimeEpochOffset != MISSING_OFFSET)
            {
                buffer.putLong(messageOffset + sendingTimeEpochOffset, requestTimestamp(), LITTLE_ENDIAN);
            }
        }

        return position;
    }

    public void commit()
    {
        proxy.commit();

        sentMessage();
    }

    private void sentMessage()
    {
        nextSendMessageTimeInMs = nextTimeoutInMs();
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
        final State state = this.state;
        if (state != State.ESTABLISHED && state != State.AWAITING_KEEPALIVE)
        {
            throw new IllegalStateException(
                "State should be ESTABLISHED or AWAITING_KEEPALIVE in order to send but is " + state);
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

    public int nextSentSeqNo()
    {
        return nextSentSeqNo;
    }

    public void nextSentSeqNo(final int nextSentSeqNo)
    {
        this.nextSentSeqNo = nextSentSeqNo;
    }

    public int nextRecvSeqNo()
    {
        return nextRecvSeqNo;
    }

    public void nextRecvSeqNo(final int nextRecvSeqNo)
    {
        this.nextRecvSeqNo = nextRecvSeqNo;
    }

    public long retransmitFillSeqNo()
    {
        return retransmitFillSeqNo;
    }

    // END PUBLIC API

    public long nextReceiveMessageTimeInMs()
    {
        return nextReceiveMessageTimeInMs;
    }

    public long nextSendMessageTimeInMs()
    {
        return nextSendMessageTimeInMs;
    }

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
        return TimeUtil.microSecondTimestamp();
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
            resendTime = nextTimeoutInMs();
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
            resendTime = nextTimeoutInMs();
            state = State.SENT_ESTABLISH;
            return true;
        }

        return false;
    }

    private long nextTimeoutInMs()
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
                    onNegotiateFailure();
                    fullyUnbind();
                    return 1;
                }
                break;

            case NEGOTIATED:
                return sendEstablish() ? 1 : 0;

            case RETRY_ESTABLISH:
                if (timeInMs > resendTime)
                {
                    onEstablishFailure();
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

            case ESTABLISHED:
            {
                if (timeInMs > nextReceiveMessageTimeInMs)
                {
                    sendSequence(Lapsed);

                    onReceivedMessage();

                    this.state = State.AWAITING_KEEPALIVE;
                }
                else if (timeInMs > nextSendMessageTimeInMs)
                {
                    sendSequence(NotLapsed);
                }
                break;
            }

            case AWAITING_KEEPALIVE:
            {
                if (timeInMs > nextReceiveMessageTimeInMs)
                {
                    final int expiry = 2 * configuration.requestedKeepAliveIntervalInMs();
                    terminate(expiry + "ms expired without message", 0);
                }
                break;
            }

            case UNBINDING:
            {
                if (timeInMs > nextSendMessageTimeInMs)
                {
                    fullyUnbind();
                }
                break;
            }
        }
        return 0;
    }

    private void onNegotiateFailure()
    {
        initiateReply.onError(new TimeoutException("Timed out: no reply for Negotiate"));
    }

    private void onEstablishFailure()
    {
        initiateReply.onError(new TimeoutException("Timed out: no reply for Establish"));
    }

    private void sendSequence(final KeepAliveLapsed keepAliveIntervalLapsed)
    {
        final long position = proxy.sendSequence(uuid, nextSentSeqNo, FTI.Primary, keepAliveIntervalLapsed);
        if (position > 0)
        {
            sentMessage();
        }

        // Will be retried on next poll if enqueue back pressured.
    }

    // EVENT HANDLERS

    public long onNegotiationResponse(
        final long uUID,
        final long requestTimestamp,
        final int secretKeySecureIDExpiration,
        final long previousSeqNo,
        final long previousUUID)
    {
        if (uUID != uuid())
        {
            // TODO: error
            System.out.println("onNegotiationResponse uUID = " + uUID);
        }

        // TODO: validate request timestamp
        // TODO: calculate session expiration
        // TODO: check gap with previous sequence number and uuid

        state = State.NEGOTIATED;
        sendEstablish();

        return 1; // TODO: move to action
    }

    public long onNegotiationReject(
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

    public long onEstablishmentAck(
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
            System.out.println("onNegotiationResponse uUID = " + uUID);
        }

        // TODO: validate request timestamp
        // TODO: calculate session expiration
        // TODO: check gap with previous sequence number and uuid

        state = State.ESTABLISHED;
        initiateReply.onComplete(this);

        nextReceiveMessageTimeInMs = nextSendMessageTimeInMs = nextTimeoutInMs();

        return 1;
    }

    public long onEstablishmentReject(
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

    public long onTerminate(final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
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

    public long onSequence(
        final long uUID, final long nextSeqNo, final FTI fti, final KeepAliveLapsed keepAliveLapsed)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // if (nextSeqNo != TODO)

        // Reply to any warning messages to keep the session alive.
        if (keepAliveLapsed == Lapsed)
        {
            sendSequence(NotLapsed);
        }

        onReceivedMessage();

        return 1;
    }

    public long onNotApplied(final long uUID, final long fromSeqNo, final long msgCount)
    {
        if (uUID != uuid())
        {
            // TODO: error
        }

        // Stop messages from being sent whilst a retransmit is underway.
        state = State.RETRANSMITTING;

        handler.onNotApplied(fromSeqNo, msgCount, response);

        onReceivedMessage();

        if (response.shouldRetransmit())
        {
            // TODO: handle backpressure better
            return inboundPublication.saveValidResendRequest(
                uUID,
                connectionId,
                fromSeqNo,
                fromSeqNo + msgCount - 1,
                0,
                NO_BUFFER,
                0,
                0);
        }
        else
        {
            sendSequence(NotLapsed);

            state = State.ESTABLISHED;

            return 1;
        }
    }

    void onReplayComplete()
    {
        state = State.ESTABLISHED;
    }

    private void onReceivedMessage()
    {
        nextReceiveMessageTimeInMs = nextTimeoutInMs();
    }

    private void fullyUnbind()
    {
        // TODO: linger state = State.SENT_TERMINATE;
        state = State.UNBOUND;
        requestDisconnect(LOGOUT);
        owner.onUnbind(this);
    }

    public long onMessage(
        final DirectBuffer buffer, final int offset, final int templateId, final int blockLength, final int version)
    {
        onReceivedMessage();

        final int seqNum = offsets.seqNum(templateId, buffer, offset);
        if (seqNum == MISSING_OFFSET)
        {
            return 1;
        }

        final int possRetrans = offsets.possRetrans(templateId, buffer, offset);
        if (possRetrans == BOOLEAN_FLAG_TRUE)
        {
            if (seqNum == retransmitFillSeqNo)
            {
                final RetransmitRequest retransmitRequest = retransmitRequests.peekFirst();
                if (retransmitRequest == null)
                {
                    retransmitFillSeqNo = NOT_AWAITING_RETRANSMIT;
                }
                else
                {
                    final long fromSeqNo = retransmitRequest.fromSeqNo;
                    final int msgCount = retransmitRequest.msgCount;
                    final long position = sendRetransmitRequest(fromSeqNo, msgCount);

                    if (!Pressure.isBackPressured(position))
                    {
                        retransmitRequests.pollFirst();
                        retransmitFillSeqNo = fromSeqNo + msgCount - 1;
                    }

                    return position;
                }
            }

            return 1;
        }

        if (seqNum == nextRecvSeqNo)
        {
            nextRecvSeqNo = seqNum + 1;

            return 1;
        }

        final long fromSeqNo = nextRecvSeqNo;
        final int msgCount = seqNum - nextRecvSeqNo;

        if (retransmitFillSeqNo == NOT_AWAITING_RETRANSMIT)
        {
            final long position = sendRetransmitRequest(fromSeqNo, msgCount);
            if (!Pressure.isBackPressured(position))
            {
                nextRecvSeqNo = seqNum + 1;
                retransmitFillSeqNo = fromSeqNo + msgCount - 1;
            }
            return position;
        }
        else
        {
            retransmitRequests.offerLast(new RetransmitRequest(fromSeqNo, msgCount));
            nextRecvSeqNo = seqNum + 1;

            return 1;
        }
    }

    private long sendRetransmitRequest(final long fromSeqNo, final int msgCount)
    {
        sentMessage();
        final long requestTimestamp = nanoSecondTimestamp();
        return proxy.sendRetransmitRequest(uuid, requestTimestamp, fromSeqNo, msgCount);
    }

    static final class RetransmitRequest
    {
        final long fromSeqNo;
        final int msgCount;

        RetransmitRequest(final long fromSeqNo, final int msgCount)
        {
            this.fromSeqNo = fromSeqNo;
            this.msgCount = msgCount;
        }
    }
}
