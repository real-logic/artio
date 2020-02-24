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

import uk.co.real_logic.artio.ilink.AbstractILink3Proxy;
import uk.co.real_logic.artio.ilink.ILink3EndpointHandler;
import uk.co.real_logic.artio.protocol.GatewayPublication;

/**
 * External users should never rely on this API.
 */
public class InternalILink3Session extends ILink3Session implements ILink3EndpointHandler
{
    public InternalILink3Session(
        final AbstractILink3Proxy proxy,
        final ILink3SessionConfiguration configuration,
        final long connectionId,
        final InitiateILink3SessionReply initiateReply,
        final GatewayPublication outboundPublication,
        final int libraryId,
        final LibraryPoller owner)
    {
        super(proxy, configuration, connectionId, initiateReply, outboundPublication, libraryId, owner);
    }

    public long onNegotiationResponse(
        final long uUID,
        final long requestTimestamp,
        final int secretKeySecureIDExpiration,
        final long previousSeqNo,
        final long previousUUID)
    {
        return super.onNegotiationResponse(
            uUID, requestTimestamp, secretKeySecureIDExpiration, previousSeqNo, previousUUID);
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
        return super.onEstablishmentAck(
            uUID,
            requestTimestamp,
            nextSeqNo,
            previousSeqNo,
            previousUUID,
            keepAliveInterval,
            secretKeySecureIDExpiration);
    }

    public long onTerminate(final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
    {
        return super.onTerminate(reason, uUID, requestTimestamp, errorCodes);
    }

    public long onNegotiationReject(
        final String reason, final long uUID, final long requestTimestamp, final int errorCodes)
    {
        return super.onNegotiationReject(reason, uUID, requestTimestamp, errorCodes);
    }

    public long onEstablishmentReject(
        final String reason, final long uUID, final long requestTimestamp, final long nextSeqNo, final int errorCodes)
    {
        return super.onEstablishmentReject(reason, uUID, requestTimestamp, nextSeqNo, errorCodes);
    }

    public int poll(final long timeInMs)
    {
        return super.poll(timeInMs);
    }
}
