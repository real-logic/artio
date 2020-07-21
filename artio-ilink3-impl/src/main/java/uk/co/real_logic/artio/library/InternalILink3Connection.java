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

import iLinkBinary.BusinessReject521Decoder;
import iLinkBinary.FTI;
import iLinkBinary.KeepAliveLapsed;
import io.aeron.exceptions.TimeoutException;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.ilink.ILink3ConnectionHandler;
import uk.co.real_logic.artio.ilink.ILink3Offsets;
import uk.co.real_logic.artio.ilink.ILink3Proxy;
import uk.co.real_logic.artio.ilink.IllegalResponseException;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.CharFormatter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.function.Consumer;

import static iLinkBinary.KeepAliveLapsed.Lapsed;
import static iLinkBinary.KeepAliveLapsed.NotLapsed;
import static iLinkBinary.RetransmitRequest508Decoder.lastUUIDNullValue;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.artio.LogTag.ILINK_SESSION;
import static uk.co.real_logic.artio.ilink.AbstractILink3Offsets.MISSING_OFFSET;
import static uk.co.real_logic.artio.ilink.AbstractILink3Parser.BOOLEAN_FLAG_TRUE;
import static uk.co.real_logic.artio.library.ILink3ConnectionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.DisconnectReason.FAILED_AUTHENTICATION;
import static uk.co.real_logic.artio.messages.DisconnectReason.LOGOUT;

/**
 * External users should never rely on this API.
 */
public final class InternalILink3Connection extends ILink3Connection
{
    private static final int KEEP_ALIVE_INTERVAL_LAPSED_ERROR_CODE = 20;

    private static final String[] TERMINATE_ERROR_CODES = {
        "Finished: session is being terminated without finalization",
        "Unknown",
        "Unnegotiated: Sending any message when session has not been Negotiated",
        "NotEstablished: Sending any message (except Negotiate) when session has not been established",
        "AlreadyNegotiated: NegotiationResponse was already sent; Negotiate was redundant",
        "NegotiationInProgress: Previous Negotiate still being processed; Wait for NegotiationResponse or timeout",
        "AlreadyEstablished: EstablishmentAck was already sent; Establish was redundant",
        "EstablishInProgress: Previous Establish still being processed; Wait for EstablishmentAck or timeout",
        "CMEAdministeredPortClosure: due to invalid number of Negotiate/Establish attempts being exceeded",
        "Volume Controls - exceeding TPS limit as defined for volume controls (logout action)",
        "InvalidNextSeqNo - value is not greater than the one last used for same UUID or value sent by the client is" +
            " out of acceptable range (MIN, MAX)",
        "InvalidMsgSeqNo - value is lower than the last one used for the same UUID or value is not initialized to 1" +
            " at the beginning of the week or value sent by the client is out of acceptable range (MIN, MAX)",
        "InvalidLastSeqNo - value is lower than the last one used for the same UUID or value sent by the client is" +
            " out of acceptable range (MIN, MAX)",
        "InvalidUUID: UUID value does not match current UUID or value sent by the client is out of acceptable" +
            " range (MIN, MAX)",
        "InvalidTimestamp: Timestamp value does not match with RequestTimestamp sent by CME or value sent by the" +
            " client is out of acceptable range (MIN, MAX)",
        "RequiredUUIDMisssing: null value in UUID field",
        "RequiredRequestTimestampMissing: null value in RequestTimestamp field",
        "RequiredCodeMisssing: null value in Code field",
        "InvalidSOFH: Invalid message length or invalid encoding type specified",
        "DecodingError: Incoming message could not be decoded",
        "KeepAliveIntervalLapsed: KeepAliveInterval has lapsed without any response so terminating session",
        "RequiredNextSeqNoMissing: null value in NextSeqNo field",
        "RequiredKeepAliveIntervalLapsedMissing: null value in KeepAliveIntervalLapsed field",
        "Non-Negotiate/Establish message received when not Negotiated/Established",
        "TerminateInProgress: Previous Terminate still being processed; Wait for Terminate or timeout",
        "Other: any other error condition not mentioned above",
        "DisconnectFromPrimary: Backup session will be terminated as well",
    };

    private static final String[] ESTABLISH_AND_NEGOTIATE_REJECT_ERROR_CODES = {
        "HMACNotAuthenticated: failed authentication because identity is not recognized",
        "HMACNotAvailable: HMAC component is not responding (5sec)",
        "InvalidUUID: UUID is not greater than the one last used or value sent by the client is out of acceptable" +
            " range (MIN, MAX)",
        "InvalidTimestamp: Timestamp is not greater than the one last used or value sent by the client is out of" +
            " acceptable range (MIN, MAX)",
        "RequiredHMACSignatureMissing: empty bytes in HMACSignature field",

        "RequiredAccessKeyIDMissing: empty bytes in AccessKeyID field",
        "RequiredSessionMissing: empty bytes in Session field",
        "RequiredFirmMissing: empty bytes in Firm field",
        "RequiredUUIDMisssing: null value in UUID field",
        "RequiredRequestTimestampMissing: null value in RequestTimestamp field",

        "SessionBlocked: session and firm are not authorized for this port",
        "InvalidKeepAliveInterval: value is out of acceptable range (MIN, MAX)",
        "InvalidAccessKeyID: contains non-printable ASCII character",
        "InvalidSession: contains non-printable ASCII character",
        "InvalidFirm: contains non-printable ASCII character",

        "Volume Controls - exceeding TPS limit as defined for volume controls (reject action)",
        "SplitMessageRejected - Messages queued due to split message penalty being rejected because of" +
            " logout or disconnect",
        "SplitMessageQueue - Reached threshold of messages queued due to split message penalty",
        "RequiredTradingSystemNameMissing: empty bytes in TradingSystemName",
        "RequiredTradingSystemVersionMissing: empty bytes in TradingSystemVersion",

        "RequiredTradingSystemVendorMissing: empty bytes in TradingSystemVendor",
        "RequiredKeepAliveIntervalMissing: null value in KeepAliveInterval field",
        "RequiredNextSeqNoMissing: empty bytes in NextSeqNo field",
        "InvalidTradingSystemName: contains non-prinatable ASCII character",
        "InvalidTradingSystemVersion: contains non-prinatable ASCII character",

        "InvalidTradingSystemVendor: contains non-prinatable ASCII character",
        "26: Unknown",
        "DesignatedBackup - Using Designated backup before designated primary not allowed",
        "NegotiateNotAllowed - Not allowed to negotiate on backup when established on primary"
    };

    private static final UnsafeBuffer NO_BUFFER = new UnsafeBuffer();
    private static final long OK_POSITION = Long.MIN_VALUE;

    private final NotAppliedResponse response = new NotAppliedResponse();
    private final Deque<RetransmitRequest> retransmitRequests = new ArrayDeque<>();
    private final CharFormatter unknownMessage = new CharFormatter(
        "Unknown Message,templateId=%s,blockLength=%s,version=%s,seqNum=%s,possRetrans=%s%n");
    private final CharFormatter checkSeqNum = new CharFormatter("Checking msgSeqNum=%s,nextRecvSeqNo=%s%n");
    private final CharFormatter retransmitFilled =
        new CharFormatter("RetransmitFilled retransmitFillSeqNo=%s%n");
    private final CharFormatter retransmitFilledNext = new CharFormatter(
        "RetransmitFilledNext uuid=%s,lastUuid=%s,retransmitFillSeqNo=%s,fromSeqNo=%s,msgCount=%s%n");

    private final BusinessReject521Decoder businessReject = new BusinessReject521Decoder();
    private final Consumer<StringBuilder> businessRejectAppendTo = businessReject::appendTo;

    private final ILink3Proxy proxy;
    private final ILink3Offsets offsets;
    private final ILink3ConnectionConfiguration configuration;
    private final long connectionId;
    private final GatewayPublication outboundPublication;
    private final GatewayPublication inboundPublication;
    private final int libraryId;
    private final LibraryPoller owner;
    private final ILink3ConnectionHandler handler;
    private final boolean newlyAllocated;
    private final long uuid;
    private final EpochNanoClock epochNanoClock;

    private final long lastUuid;
    private final long lastConnectionLastReceivedSequenceNumber;

    private InitiateILink3ConnectionReply initiateReply;

    private State state;
    private long nextRecvSeqNo;
    private long nextSentSeqNo;

    private long retransmitFillSeqNo = NOT_AWAITING_RETRANSMIT;
    private long nextRetransmitSeqNo = NOT_AWAITING_RETRANSMIT;

    private long resendTime;
    private long nextReceiveMessageTimeInMs;
    private long nextSendMessageTimeInMs;
    private boolean backpressuredNotApplied = false;

    private String resendTerminateReason;
    private int resendTerminateErrorCodes;
    private long lastNegotiateRequestTimestamp;
    private long lastEstablishRequestTimestamp;

    public InternalILink3Connection(
        final ILink3ConnectionConfiguration configuration,
        final long connectionId,
        final InitiateILink3ConnectionReply initiateReply,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final int libraryId,
        final LibraryPoller owner,
        final long uuid,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final boolean newlyAllocated,
        final long lastUuid,
        final EpochNanoClock epochNanoClock)
    {
        this.configuration = configuration;
        this.connectionId = connectionId;
        this.initiateReply = initiateReply;
        this.outboundPublication = outboundPublication;
        this.inboundPublication = inboundPublication;
        this.libraryId = libraryId;
        this.owner = owner;
        this.handler = configuration.handler();
        this.newlyAllocated = newlyAllocated;
        this.epochNanoClock = epochNanoClock;

        proxy = new ILink3Proxy(connectionId, outboundPublication.dataPublication());
        offsets = new ILink3Offsets();
        nextSentSeqNo(calculateInitialSequenceNumber(
            lastSentSequenceNumber, configuration.initialSentSequenceNumber()));
        nextRecvSeqNo(calculateInitialSequenceNumber(
            lastReceivedSequenceNumber, configuration.initialReceivedSequenceNumber()));

        this.uuid = uuid;
        this.lastUuid = lastUuid;
        this.lastConnectionLastReceivedSequenceNumber = calculateSequenceNumber(lastReceivedSequenceNumber);

        state = State.CONNECTED;
    }

    private long calculateSequenceNumber(final long lookedUpSequenceNumber)
    {
        return lookedUpSequenceNumber == Session.UNKNOWN ? 0 : lookedUpSequenceNumber;
    }

    // PUBLIC API

    public long tryClaim(
        final MessageEncoderFlyweight message)
    {
        return tryClaim(message, 0);
    }

    public long tryClaim(
        final MessageEncoderFlyweight message, final int variableLength)
    {
        validateCanSend();

        final long position = proxy.claimILinkMessage(
            message.sbeBlockLength() + variableLength, message);

        if (position > 0)
        {
            final int templateId = message.sbeTemplateId();
            final MutableDirectBuffer buffer = message.buffer();
            final int messageOffset = message.offset();

            final int seqNumOffset = offsets.seqNumOffset(templateId);
            if (seqNumOffset != MISSING_OFFSET)
            {
                buffer.putInt(messageOffset + seqNumOffset, (int)nextSentSeqNo++, LITTLE_ENDIAN);
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

    public void abort()
    {
        proxy.abort();
    }

    private void sentMessage()
    {
        nextSendMessageTimeInMs = nextTimeoutInMs();
    }

    public long terminate(final String reason, final int errorCodes)
    {
        validateCanSend();

        return sendTerminate(reason, errorCodes, State.UNBINDING, State.RESEND_TERMINATE);
    }

    public long tryRetransmitRequest(final long uuid, final long fromSeqNo, final int msgCount)
    {
        final int retransmitRequestMessageLimit = configuration.retransmitRequestMessageLimit();
        if (msgCount > retransmitRequestMessageLimit)
        {
            throw new IllegalArgumentException(
                "msgCount [" + msgCount + "] cannot be larger than " + retransmitRequestMessageLimit);
        }

        sentMessage();
        final long requestTimestamp = requestTimestamp();
        final long thisUuid = this.uuid;
        final long lastUuid = uuid == thisUuid ? lastUUIDNullValue() : uuid;
        final long position = proxy.sendRetransmitRequest(thisUuid, lastUuid, requestTimestamp, fromSeqNo, msgCount);
        if (!Pressure.isBackPressured(position))
        {
            nextRetransmitSeqNo = fromSeqNo;
            retransmitFillSeqNo = fromSeqNo + msgCount - 1;
        }
        return position;
    }

    private long sendTerminate(
        final String reason, final int errorCodes, final State finalState, final State resendState)
    {
        final long requestTimestamp = requestTimestamp();
        final long position = proxy.sendTerminate(
            reason,
            uuid,
            requestTimestamp,
            errorCodes);

        if (position > 0)
        {
            state = finalState;
            resendTerminateReason = null;
            resendTerminateErrorCodes = 0;
        }
        else
        {
            state = resendState;
            resendTerminateReason = reason;
            resendTerminateErrorCodes = errorCodes;
        }

        return position;
    }

    private void validateCanSend()
    {
        if (!canSendMessage())
        {
            throw new IllegalStateException(
                "State should be ESTABLISHED or AWAITING_KEEPALIVE in order to send but is " + state);
        }
    }

    public boolean canSendMessage()
    {
        final State state = this.state;
        return state == State.ESTABLISHED || state == State.AWAITING_KEEPALIVE;
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

    public long nextSentSeqNo()
    {
        return nextSentSeqNo;
    }

    public void nextSentSeqNo(final long nextSentSeqNo)
    {
        this.nextSentSeqNo = nextSentSeqNo;
    }

    public long nextRecvSeqNo()
    {
        return nextRecvSeqNo;
    }

    public void nextRecvSeqNo(final long nextRecvSeqNo)
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

    private long calculateInitialSequenceNumber(
        final long lastSequenceNumber, final long initialSequenceNumber)
    {
        if (!this.configuration.reEstablishLastConnection())
        {
            return 1;
        }

        if (initialSequenceNumber == AUTOMATIC_INITIAL_SEQUENCE_NUMBER)
        {
            if (lastSequenceNumber == Session.UNKNOWN)
            {
                return 1;
            }
            else
            {
                return lastSequenceNumber + 1;
            }
        }
        return initialSequenceNumber;
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
            lastNegotiateRequestTimestamp = requestTimestamp;
            return true;
        }

        return false;
    }

    private long requestTimestamp()
    {
        return epochNanoClock.nanoTime();
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
            lastEstablishRequestTimestamp = requestTimestamp;
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
            return sha256HMAC.doFinal(canonicalRequest.getBytes(StandardCharsets.UTF_8));
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
                return pollConnected();

            case SENT_NEGOTIATE:
                return pollSentNegotiate(timeInMs);

            case RETRY_NEGOTIATE:
                return pollRetryNegotiate(timeInMs);

            case NEGOTIATED:
                return sendEstablish() ? 1 : 0;

            case RETRY_ESTABLISH:
                return pollRetryEstablish(timeInMs);

            case SENT_ESTABLISH:
                return pollSentEstablish(timeInMs);

            case ESTABLISHED:
                return pollEstablished(timeInMs);

            case AWAITING_KEEPALIVE:
                return pollAwaitingKeepAlive(timeInMs);

            case RESEND_TERMINATE:
                return pollResendTerminate();

            case RESEND_TERMINATE_ACK:
                return pollResendTerminateAck();

            case UNBINDING:
                return pollUnbinding(timeInMs);

            default:
                return 0;
        }
    }

    private int pollUnbinding(final long timeInMs)
    {
        if (timeInMs > nextSendMessageTimeInMs)
        {
            fullyUnbind();
        }
        return 0;
    }

    private int pollResendTerminateAck()
    {
        sendTerminateAck(resendTerminateReason, resendTerminateErrorCodes);
        return 0;
    }

    private int pollResendTerminate()
    {
        terminate(resendTerminateReason, resendTerminateErrorCodes);
        return 0;
    }

    private int pollAwaitingKeepAlive(final long timeInMs)
    {
        if (timeInMs > nextReceiveMessageTimeInMs)
        {
            final int expiry = 2 * configuration.requestedKeepAliveIntervalInMs();
            terminate(expiry + "ms expired without message", KEEP_ALIVE_INTERVAL_LAPSED_ERROR_CODE);
        }
        return 0;
    }

    private int pollEstablished(final long timeInMs)
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
        return 0;
    }

    private int pollSentEstablish(final long timeInMs)
    {
        if (timeInMs > resendTime)
        {
            if (sendEstablish())
            {
                this.state = State.RETRY_ESTABLISH;
                return 1;
            }
        }
        return 0;
    }

    private int pollRetryEstablish(final long timeInMs)
    {
        if (timeInMs > resendTime)
        {
            onEstablishFailure();
            fullyUnbind();
            return 1;
        }
        return 0;
    }

    private int pollRetryNegotiate(final long timeInMs)
    {
        if (timeInMs > resendTime)
        {
            onNegotiateFailure();
            fullyUnbind();
            return 1;
        }
        return 0;
    }

    private int pollSentNegotiate(final long timeInMs)
    {
        if (timeInMs > resendTime)
        {
            if (sendNegotiate())
            {
                this.state = State.RETRY_NEGOTIATE;
                return 1;
            }
        }
        return 0;
    }

    private int pollConnected()
    {
        if (!configuration.reEstablishLastConnection() || newlyAllocated)
        {
            return sendNegotiate() ? 1 : 0;
        }
        else
        {
            return sendEstablish() ? 1 : 0;
        }
    }

    private void onNegotiateFailure()
    {
        initiateReply.onError(new TimeoutException("Timed out: no reply for Negotiate"));
    }

    private void onEstablishFailure()
    {
        initiateReply.onError(new TimeoutException("Timed out: no reply for Establish"));
    }

    public long trySendSequence()
    {
        return sendSequence(NotLapsed);
    }

    private long sendSequence(final KeepAliveLapsed keepAliveIntervalLapsed)
    {
        final long position = proxy.sendSequence(uuid, nextSentSeqNo, FTI.Primary, keepAliveIntervalLapsed);
        if (position > 0)
        {
            sentMessage();
        }

        // Will be retried on next poll if enqueue back pressured.
        return position;
    }

    // EVENT HANDLERS

    public long onNegotiationResponse(
        final long uUID,
        final long requestTimestamp,
        final int secretKeySecureIDExpiration,
        final long previousSeqNo,
        final long previousUUID)
    {
        if (checkBoundaryErrors("Negotiate", uUID, requestTimestamp, lastNegotiateRequestTimestamp))
        {
            return 1;
        }

        state = State.NEGOTIATED;
        sendEstablish();

        return 1;
    }

    private boolean checkBoundaryErrors(
        final String name, final long uUID, final long requestTimestamp, final long expectedRequestTimestamp)
    {
        if (uUID != uuid())
        {
            connectionError(new IllegalResponseException("Invalid " + name + ".uuid=" + uUID + ",expected=" + uuid()));
            return true;
        }

        if (expectedRequestTimestamp != requestTimestamp)
        {
            connectionError(new IllegalResponseException(
                "Invalid " + name + ".requestTimestamp=" + requestTimestamp +
                ",expected=" + expectedRequestTimestamp));
            return true;
        }

        return false;
    }

    public long onNegotiationReject(
        final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
    {
        state = State.NEGOTIATE_REJECTED;
        return onReject(
            uUID,
            requestTimestamp,
            lastNegotiateRequestTimestamp,
            "Negotiate rejected: " + reason,
            errorCodes);
    }

    private void connectionError(final Exception error)
    {
        initiateReply.onError(error);
        initiateReply = null;

        requestDisconnect(FAILED_AUTHENTICATION);
        owner.remove(this);
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
        if (checkBoundaryErrors("EstablishmentAck", uUID, requestTimestamp, lastEstablishRequestTimestamp))
        {
            return 1;
        }

        state = State.ESTABLISHED;
        initiateReply.onComplete(this);
        nextReceiveMessageTimeInMs = nextSendMessageTimeInMs = nextTimeoutInMs();

        if (previousUUID == lastUuid)
        {
            if (previousSeqNo > lastConnectionLastReceivedSequenceNumber)
            {
                final long impliedNextRecvSeqNo = previousSeqNo + 1;
                final long oldNextRecvSeqNo = lastConnectionLastReceivedSequenceNumber + 1;
                final long retransmitLastUuid = uUID == previousUUID ? lastUUIDNullValue() : previousUUID;
                return onInvalidSequenceNumber(
                    retransmitLastUuid, impliedNextRecvSeqNo, oldNextRecvSeqNo, impliedNextRecvSeqNo);
            }
        }

        final long position = checkLowSequenceNumberCase(nextSeqNo, nextRecvSeqNo);
        if (position != OK_POSITION)
        {
            return position;
        }

        return 1;
    }

    public long onEstablishmentReject(
        final String reason, final long uUID, final long requestTimestamp, final long nextSeqNo, final int errorCodes)
    {
        state = State.ESTABLISH_REJECTED;
        final String reasonMsg = "Establishment rejected: " + reason + ",nextSeqNo=" + nextSeqNo;
        return onReject(uUID, requestTimestamp, lastEstablishRequestTimestamp, reasonMsg, errorCodes);
    }

    private long onReject(
        final long msgUuid,
        final long msgRequestTimestamp,
        final long expectedRequestTimestamp,
        final String reasonMsg,
        final int errorCodes)
    {
        final StringBuilder msgBuilder = new StringBuilder(reasonMsg);
        if (msgUuid != uuid)
        {
            msgBuilder
                .append("Incorrect uuid=")
                .append(msgUuid)
                .append(",expected=")
                .append(uuid)
                .append(",");
        }
        if (msgRequestTimestamp != expectedRequestTimestamp)
        {
            msgBuilder
                .append("Incorrect requestTimestamp=")
                .append(msgRequestTimestamp)
                .append(",expected=")
                .append(expectedRequestTimestamp)
                .append(",");
        }

        msgBuilder
            .append(",errorCodes=")
            .append(errorCodes)
            .append(",errorMessage=")
            .append(ESTABLISH_AND_NEGOTIATE_REJECT_ERROR_CODES[errorCodes]);
        connectionError(new IllegalResponseException(msgBuilder.toString()));

        return 1;
    }

    public long onTerminate(final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
    {
        // We initiated termination
        if (state == State.UNBINDING)
        {
            fullyUnbind();
        }
        // The exchange initiated termination
        else
        {
            if (errorCodes >= 2 && errorCodes < TERMINATE_ERROR_CODES.length)
            {
                DebugLogger.log(ILINK_SESSION, TERMINATE_ERROR_CODES[errorCodes]);
            }
            sendTerminateAck(reason, errorCodes);
        }

        checkUuid(uUID);

        return 1;
    }

    private void sendTerminateAck(final String reason, final int errorCodes)
    {
        final long position = sendTerminate(reason, errorCodes, State.UNBOUND, State.RESEND_TERMINATE_ACK);
        if (position > 0)
        {
            fullyUnbind();
        }
    }

    private void checkUuid(final long uUID)
    {
        if (uUID != uuid())
        {
            handler.onError(new IllegalResponseException("Invalid uuid=" + uUID + ",expected=" + uuid()));
        }
    }

    public long onSequence(
        final long uUID, final long nextSeqNo, final FTI fti, final KeepAliveLapsed keepAliveLapsed)
    {
        if (uUID == uuid())
        {
            onReceivedMessage();

            final long position = checkLowSequenceNumberCase(nextSeqNo, nextRecvSeqNo);
            if (position == OK_POSITION)
            {
                final long expectedNextRecvSeqNo = this.nextRecvSeqNo;
                nextRecvSeqNo(nextSeqNo);

                if (expectedNextRecvSeqNo < nextSeqNo)
                {
                    // sequence gap, initiate retransmission.
                    return onInvalidSequenceNumber(lastUUIDNullValue(), nextSeqNo, expectedNextRecvSeqNo, nextSeqNo);
                }
            }
            else
            {
                // low sequence number triggered disconnect
                handler.onSequence(uUID, nextSeqNo);
                return position;
            }

            // Reply to any warning messages to keep the session alive.
            if (keepAliveLapsed == Lapsed)
            {
                sendSequence(NotLapsed);
            }

            handler.onSequence(uUID, nextSeqNo);
        }

        return 1;
    }

    private long checkLowSequenceNumberCase(final long seqNo, final long nextRecvSeqNo)
    {
        if (seqNo < nextRecvSeqNo)
        {
            return terminate(String.format(
                "seqNo=%s,expecting=%s",
                seqNo,
                this.nextRecvSeqNo), 0);
        }

        return OK_POSITION;
    }

    public long onNotApplied(final long uUID, final long fromSeqNo, final long msgCount)
    {
        if (uUID != uuid())
        {

        }

        // Don't invoke the handler on the backpressured retry
        if (!backpressuredNotApplied)
        {
            // Stop messages from being sent whilst a retransmit is underway.
            state = State.RETRANSMITTING;
            handler.onNotApplied(fromSeqNo, msgCount, response);
            onReceivedMessage();
        }

        if (response.shouldRetransmit())
        {
            final long position = inboundPublication.saveValidResendRequest(
                uUID,
                connectionId,
                fromSeqNo,
                fromSeqNo + msgCount - 1,
                0,
                NO_BUFFER,
                0,
                0);

            backpressuredNotApplied = Pressure.isBackPressured(position);

            return position;
        }
        else
        {
            final long position = sendSequence(NotLapsed);
            if (position > 0)
            {
                state = State.ESTABLISHED;
            }

            backpressuredNotApplied = Pressure.isBackPressured(position);

            return position;
        }
    }

    void onReplayComplete()
    {
        state = State.ESTABLISHED;
    }

    private void onReceivedMessage()
    {
        if (state == State.AWAITING_KEEPALIVE)
        {
            state = State.ESTABLISHED;
        }

        nextReceiveMessageTimeInMs = nextTimeoutInMs();
    }

    void fullyUnbind()
    {
        requestDisconnect(LOGOUT);
        owner.remove(this);
        unbindState();
    }

    void unbindState()
    {
        state = State.UNBOUND;
        handler.onDisconnect();
    }

//    private

    public long onMessage(
        final DirectBuffer buffer, final int offset, final int templateId, final int blockLength, final int version)
    {
        onReceivedMessage();

        if (state == State.ESTABLISHED)
        {
            final long seqNum = offsets.seqNum(templateId, buffer, offset);
            if (seqNum == MISSING_OFFSET)
            {
                return 1;
            }

            final int possRetrans = offsets.possRetrans(templateId, buffer, offset);
            if (possRetrans == BOOLEAN_FLAG_TRUE)
            {
                if (seqNum > nextRetransmitSeqNo)
                {
                    final long position = onInvalidSequenceNumber(
                        lastUUIDNullValue(), seqNum, nextRetransmitSeqNo, nextRecvSeqNo);
                    if (Pressure.isBackPressured(position))
                    {
                        return position;
                    }
                }

                handler.onBusinessMessage(templateId, buffer, offset, blockLength, version, true);

                if (seqNum == retransmitFillSeqNo)
                {
                    return retransmitFilled();
                }
                else
                {
                    nextRetransmitSeqNo = seqNum + 1;
                }

                return 1;
            }

            final long nextRecvSeqNo = this.nextRecvSeqNo;
            DebugLogger.log(ILINK_SESSION, checkSeqNum, seqNum, nextRecvSeqNo);
            final long position = checkLowSequenceNumberCase(seqNum, nextRecvSeqNo);
            if (position == OK_POSITION)
            {
                if (nextRecvSeqNo == seqNum)
                {
                    nextRecvSeqNo(seqNum + 1);

                    checkBusinessRejectSequenceNumber(buffer, offset, templateId, blockLength, version);
                    handler.onBusinessMessage(templateId, buffer, offset, blockLength, version, false);

                    return 1;
                }
                else /* nextRecvSeqNo > seqNum */
                {
                    // We could queue this instead of just passing it on to the customer's application but this
                    // hasn't been requested as of yet
                    checkBusinessRejectSequenceNumber(buffer, offset, templateId, blockLength, version);
                    handler.onBusinessMessage(templateId, buffer, offset, blockLength, version, false);

                    return onInvalidSequenceNumber(seqNum);
                }
            }
            else
            {
                return position;
            }
        }
        else
        {
            final long seqNum = offsets.seqNum(templateId, buffer, offset);
            final boolean possRetrans = offsets.possRetrans(templateId, buffer, offset) == BOOLEAN_FLAG_TRUE;

            if (DebugLogger.isEnabled(ILINK_SESSION))
            {
                unknownMessage.clear()
                    .with(templateId)
                    .with(blockLength)
                    .with(version)
                    .with(seqNum)
                    .with(possRetrans);
                DebugLogger.log(ILINK_SESSION, unknownMessage);

                if (templateId == BusinessReject521Decoder.TEMPLATE_ID)
                {
                    businessReject.wrap(buffer, offset, blockLength, version);
                    DebugLogger.logSbeDecoder(ILINK_SESSION, "> ", businessRejectAppendTo);
                }
            }

            return 1;
        }
    }

    private void checkBusinessRejectSequenceNumber(
        final DirectBuffer buffer, final int offset, final int templateId, final int blockLength, final int version)
    {
        if (templateId == BusinessReject521Decoder.TEMPLATE_ID)
        {
            businessReject.wrap(buffer, offset, blockLength, version);
            if (businessReject.refSeqNum() == BusinessReject521Decoder.refSeqNumNullValue())
            {
                nextSentSeqNo--;
            }
        }
    }

    private long onInvalidSequenceNumber(final long seqNum)
    {
        return onInvalidSequenceNumber(seqNum, seqNum + 1);
    }

    private long onInvalidSequenceNumber(final long msgSeqNum, final long newNextRecvSeqNo)
    {
        return onInvalidSequenceNumber(lastUUIDNullValue(), msgSeqNum, nextRecvSeqNo, newNextRecvSeqNo);
    }

    private long onInvalidSequenceNumber(
        final long lastUuid, final long msgSeqNum, final long oldNextRecvSeqNo, final long newNextRecvSeqNo)
    {
        final long fromSeqNo = oldNextRecvSeqNo;
        final int totalMsgCount = (int)(msgSeqNum - oldNextRecvSeqNo);
        final int msgCount = Math.min(totalMsgCount, configuration.retransmitRequestMessageLimit());

        if (retransmitFillSeqNo == NOT_AWAITING_RETRANSMIT)
        {
            final long position = sendRetransmitRequest(lastUuid, fromSeqNo, msgCount);
            if (!Pressure.isBackPressured(position))
            {
                addRemainingRetransmitRequests(lastUuid, fromSeqNo, msgCount, totalMsgCount);
                nextRecvSeqNoForCurrentUuid(newNextRecvSeqNo, lastUuid);
                nextRetransmitSeqNo = fromSeqNo;
                retransmitFillSeqNo = fromSeqNo + msgCount - 1;
            }
            return position;
        }
        else
        {
            addRetransmitRequest(lastUuid, fromSeqNo, msgCount);
            addRemainingRetransmitRequests(lastUuid, fromSeqNo, msgCount, totalMsgCount);
            nextRecvSeqNoForCurrentUuid(newNextRecvSeqNo, lastUuid);

            return 1;
        }
    }

    private void nextRecvSeqNoForCurrentUuid(final long newNextRecvSeqNo, final long lastUuid)
    {
        // we don't want to update newNextRecvSeqNo for an On EstablishAck retransmit request for the last uuid
        if (lastUuid == lastUUIDNullValue())
        {
            nextRecvSeqNo(newNextRecvSeqNo);
        }
    }

    private long retransmitFilled()
    {
        final RetransmitRequest retransmitRequest = retransmitRequests.peekFirst();
        if (retransmitRequest == null)
        {
            DebugLogger.log(ILINK_SESSION, retransmitFilled, retransmitFillSeqNo);
            nextRetransmitSeqNo = NOT_AWAITING_RETRANSMIT;
            retransmitFillSeqNo = NOT_AWAITING_RETRANSMIT;
        }
        else
        {
            final long lastUuid = retransmitRequest.lastUuid;
            final long fromSeqNo = retransmitRequest.fromSeqNo;
            final int msgCount = retransmitRequest.msgCount;
            final long position = sendRetransmitRequest(
                lastUuid, fromSeqNo, msgCount);

            if (!Pressure.isBackPressured(position))
            {
                if (DebugLogger.isEnabled(ILINK_SESSION))
                {
                    retransmitFilledNext
                        .clear()
                        .with(uuid)
                        .with(lastUuid)
                        .with(retransmitFillSeqNo)
                        .with(fromSeqNo)
                        .with(msgCount);
                    DebugLogger.log(ILINK_SESSION, retransmitFilledNext);
                }
                retransmitRequests.pollFirst();
                nextRetransmitSeqNo = fromSeqNo;
                retransmitFillSeqNo = fromSeqNo + msgCount - 1;
            }

            return position;
        }

        return 1;
    }

    private void addRemainingRetransmitRequests(
        final long lastUuid,
        final long initialFromSeqNo,
        final int initialMessagesRequested,
        final int totalMessageCount)
    {
        final int retransmitRequestMsgLimit = configuration.retransmitRequestMessageLimit();

        long fromSeqNo = initialFromSeqNo + initialMessagesRequested;
        int messagesRequested = initialMessagesRequested;

        while (messagesRequested < totalMessageCount)
        {
            final int msgCount = Math.min(totalMessageCount - messagesRequested, retransmitRequestMsgLimit);
            addRetransmitRequest(lastUuid, fromSeqNo, msgCount);

            messagesRequested += msgCount;
            fromSeqNo += msgCount;
        }
    }

    private void addRetransmitRequest(final long lastUuid, final long fromSeqNo, final int msgCount)
    {
        retransmitRequests.offerLast(new RetransmitRequest(lastUuid, fromSeqNo, msgCount));
    }

    private long sendRetransmitRequest(final long lastUuid, final long fromSeqNo, final int msgCount)
    {
        sentMessage();
        final long requestTimestamp = requestTimestamp();
        return proxy.sendRetransmitRequest(uuid, lastUuid, requestTimestamp, fromSeqNo, msgCount);
    }

    public long onRetransmission(
        final long uUID, final long lastUUID, final long requestTimestamp, final long fromSeqNo, final int msgCount)
    {
        // TODO: validate that
        return 1;
    }

    static final class RetransmitRequest
    {
        final long lastUuid;
        final long fromSeqNo;
        final int msgCount;

        RetransmitRequest(final long lastUuid, final long fromSeqNo, final int msgCount)
        {
            this.lastUuid = lastUuid;
            this.fromSeqNo = fromSeqNo;
            this.msgCount = msgCount;
        }
    }

    public long onRetransmitReject(
        final String reason, final long uuid, final long lastUuid, final long requestTimestamp, final int errorCodes)
    {
        checkUuid(uuid);

        handler.onRetransmitReject(reason, lastUuid, requestTimestamp, errorCodes);

        retransmitFilled();

        return 1;
    }

    public String toString()
    {
        return "InternalILink3Session{" +
            "configuration=" + configuration +
            ", connectionId=" + connectionId +
            ", libraryId=" + libraryId +
            ", uuid=" + uuid +
            ", state=" + state +
            ", nextRecvSeqNo=" + nextRecvSeqNo +
            ", nextSentSeqNo=" + nextSentSeqNo +
            ", retransmitFillSeqNo=" + retransmitFillSeqNo +
            ", nextRetransmitSeqNo=" + nextRetransmitSeqNo +
            ", nextReceiveMessageTimeInMs=" + nextReceiveMessageTimeInMs +
            ", nextSendMessageTimeInMs=" + nextSendMessageTimeInMs +
            '}';
    }
}
