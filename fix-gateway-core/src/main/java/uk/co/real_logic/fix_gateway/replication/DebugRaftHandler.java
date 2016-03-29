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

import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

/**
 * A wrapper around a raft handler that can log out all the control protocol messages.
 */
public class DebugRaftHandler implements RaftHandler
{
    private final short nodeId;
    private final RaftHandler delegateHandler;

    public static RaftHandler wrap(final short nodeId, final RaftHandler delegateHandler)
    {
        return CommonConfiguration.DEBUG_PRINT_MESSAGES
             ? new DebugRaftHandler(nodeId, delegateHandler)
             : delegateHandler;
    }

    public DebugRaftHandler(final short nodeId, final RaftHandler delegateHandler)
    {
        this.nodeId = nodeId;
        this.delegateHandler = delegateHandler;
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        DebugLogger.log(
            "%2$d: MesageAcknowledgement(newAckedPos=%3$d, nodeId=%4$d, %1$s)\n",
            status,
            this.nodeId,
            newAckedPosition,
            nodeId
        );

        delegateHandler.onMessageAcknowledgement(
            newAckedPosition, nodeId, status
        );
    }

    public void onRequestVote(
        final short candidateId, final int candidateSessionId, final int leaderShipTerm, final long lastAckedPosition)
    {
        DebugLogger.log(
            "%d: RequestVote(candidateId=%d, candidateSessionId=%d, leadershipTerm=%d, lastAckedPosition=%d)\n",
            this.nodeId,
            candidateId,
            candidateSessionId,
            leaderShipTerm,
            lastAckedPosition
        );

        delegateHandler.onRequestVote(
            candidateId, candidateSessionId, leaderShipTerm, lastAckedPosition
        );
    }

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        DebugLogger.log(
            "%2$d: ReplyVote(senderNodeId=%3$d, candidateId=%4$d, leaderShipTerm=%5$d, %1$s)\n",
            vote,
            this.nodeId,
            senderNodeId,
            candidateId,
            leaderShipTerm
        );

        delegateHandler.onReplyVote(
            senderNodeId, candidateId, leaderShipTerm, vote
        );
    }

    public void onConcensusHeartbeat(
        final short nodeId, final int leaderShipTerm, final long position, final int leaderSessionId)
    {
        DebugLogger.log(
            "%d: ConcensusHeartbeat(nodeId=%d, leaderShipTerm=%d, position=%d, leaderSessionId=%d)\n",
            this.nodeId,
            nodeId,
            leaderShipTerm,
            position,
            leaderSessionId
        );

        delegateHandler.onConcensusHeartbeat(
            nodeId, leaderShipTerm, position, leaderSessionId
        );
    }

    public void onResend(
        final int leaderSessionId,
        final int leaderShipTerm,
        final long startPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        DebugLogger.log(
            "%d: Resend(leaderSessionId=%d, leaderShipTerm=%d, startPosition=%d, bodyLength=%d)\n",
            this.nodeId,
            leaderSessionId,
            leaderShipTerm,
            startPosition,
            bodyLength
        );

        delegateHandler.onResend(
            leaderSessionId, leaderShipTerm, startPosition, bodyBuffer, bodyOffset, bodyLength
        );
    }
}
