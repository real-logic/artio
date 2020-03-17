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
package uk.co.real_logic.artio.ilink;

public interface ILink3EndpointHandler
{
    long onNegotiationResponse(
        long uUID,
        long requestTimestamp,
        int secretKeySecureIDExpiration,
        long previousSeqNo,
        long previousUUID);

    long onEstablishmentAck(
        long uUID,
        long requestTimestamp,
        long nextSeqNo,
        long previousSeqNo,
        long previousUUID,
        int keepAliveInterval,
        int secretKeySecureIDExpiration);

    long onTerminate(String reason, long uUID, long requestTimestamp, int errorCodes);

    long onNegotiationReject(String reason, long uUID, long requestTimestamp, int errorCodes);

    long onEstablishmentReject(String reason, long uUID, long requestTimestamp, long nextSeqNo, int errorCodes);

    long onSequence(long uUID, long nextSeqNo, short faultToleranceIndicator, short keepAliveIntervalLapsed);

    long onNotApplied(long uUID, long fromSeqNo, long msgCount);

}
