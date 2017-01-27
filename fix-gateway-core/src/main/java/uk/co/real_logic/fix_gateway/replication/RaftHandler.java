/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.replication.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.replication.messages.Vote;

interface RaftHandler
{
    Action onMessageAcknowledgement(long newAckedPosition, short nodeId, AcknowledgementStatus status);

    Action onRequestVote(short candidateId, int candidateSessionId, int leaderShipTerm, long lastAckedPosition);

    Action onReplyVote(
        short senderNodeId,
        short candidateId,
        int leaderShipTerm,
        Vote vote,
        DirectBuffer nodeStateBuffer,
        int nodeStateLength,
        int aeronSessionId);

    Action onConsensusHeartbeat(
        short nodeId,
        int leaderShipTerm,
        long position,
        long streamStartPosition,
        long streamPosition,
        int leaderSessionId);

    Action onResend(
        int leaderSessionId,
        int leaderShipTerm,
        long startPosition,
        DirectBuffer bodyBuffer,
        int bodyOffset,
        int bodyLength);
}
