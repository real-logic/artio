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
package uk.co.real_logic.artio.replication;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.replication.messages.AcknowledgementStatus;
import uk.co.real_logic.artio.replication.messages.Vote;

import static uk.co.real_logic.artio.LogTag.RAFT;

/**
 * A wrapper around a raft handler that can log out all the control protocol messages.
 */
final class DebugRaftHandler implements RaftHandler
{
    private final short nodeId;
    private final RaftHandler delegateHandler;

    static RaftHandler wrap(final short nodeId, final RaftHandler delegateHandler)
    {
        return CommonConfiguration.DEBUG_PRINT_MESSAGES ?
            new DebugRaftHandler(nodeId, delegateHandler) :
            delegateHandler;
    }

    private DebugRaftHandler(final short nodeId, final RaftHandler delegateHandler)
    {
        this.nodeId = nodeId;
        this.delegateHandler = delegateHandler;
    }

    public Action onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        DebugLogger.log(
            RAFT,
            "%2$d: MessageAcknowledgement(newAckedPos=%3$d, nodeId=%4$d, %1$s)%n",
            status,
            this.nodeId,
            newAckedPosition,
            nodeId);

        return delegateHandler.onMessageAcknowledgement(newAckedPosition, nodeId, status);
    }

    public Action onRequestVote(
        final short candidateId, final int candidateSessionId, final int leaderShipTerm, final long lastAckedPosition)
    {
        DebugLogger.log(
            RAFT,
            "%d: RequestVote(candidateId=%d, candidateSessionId=%d, leadershipTerm=%d, lastAckedPosition=%d)%n",
            this.nodeId,
            candidateId,
            candidateSessionId,
            leaderShipTerm,
            lastAckedPosition);

        return delegateHandler.onRequestVote(
            candidateId, candidateSessionId, leaderShipTerm, lastAckedPosition);
    }

    public Action onReplyVote(
        final short senderNodeId,
        final short candidateId,
        final int leaderShipTerm,
        final Vote vote,
        final DirectBuffer nodeStateBuffer,
        final int nodeStateLength,
        final int sessionId)
    {
        DebugLogger.log(
            RAFT,
            "%2$d: ReplyVote(senderNodeId=%3$d, candidateId=%4$d, leaderShipTerm=%5$d, %1$s)%n",
            vote,
            this.nodeId,
            senderNodeId,
            candidateId,
            leaderShipTerm);

        return delegateHandler.onReplyVote(
            senderNodeId, candidateId, leaderShipTerm, vote,
            nodeStateBuffer, nodeStateLength, sessionId);
    }

    public Action onConsensusHeartbeat(
        final short nodeId,
        final int leaderShipTerm,
        final long position,
        final long transportStartPosition,
        final long transportPosition,
        final int leaderSessionId)
    {
        DebugLogger.log(
            RAFT,
            "%d: ConsensusHeartbeat(nodeId=%d, leaderShipTerm=%d, pos=%d, " +
            "transStartPos=%d, transPos=%d, leaderSessId=%d)%n",
            this.nodeId,
            nodeId,
            leaderShipTerm,
            position,
            transportStartPosition,
            transportPosition,
            leaderSessionId);

        return delegateHandler.onConsensusHeartbeat(
            nodeId,
            leaderShipTerm,
            position,
            transportStartPosition,
            transportPosition,
            leaderSessionId);
    }

    public Action onResend(
        final int leaderSessionId,
        final int leaderShipTerm,
        final long startPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        DebugLogger.log(
            RAFT,
            "%d: Resend(leaderSessionId=%d, leaderShipTerm=%d, startPosition=%d, bodyLength=%d)%n",
            this.nodeId,
            leaderSessionId,
            leaderShipTerm,
            startPosition,
            bodyLength);

        return delegateHandler.onResend(
            leaderSessionId, leaderShipTerm, startPosition, bodyBuffer, bodyOffset, bodyLength);
    }
}
