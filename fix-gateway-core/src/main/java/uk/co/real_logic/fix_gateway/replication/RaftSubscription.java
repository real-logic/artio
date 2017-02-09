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

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import uk.co.real_logic.fix_gateway.replication.messages.*;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.replication.messages.MessageHeaderDecoder.ENCODED_LENGTH;
import static uk.co.real_logic.fix_gateway.replication.messages.ResendDecoder.bodyHeaderLength;

class RaftSubscription implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final MessageAcknowledgementDecoder messageAcknowledgement = new MessageAcknowledgementDecoder();
    private final RequestVoteDecoder requestVote = new RequestVoteDecoder();
    private final ReplyVoteDecoder replyVote = new ReplyVoteDecoder();
    private final ConsensusHeartbeatDecoder consensusHeartbeat = new ConsensusHeartbeatDecoder();
    private final ResendDecoder resend = new ResendDecoder();
    private final ExpandableArrayBuffer nodeStateBuffer = new ExpandableArrayBuffer();

    private final RaftHandler handler;

    RaftSubscription(final RaftHandler handler)
    {
        this.handler = handler;
    }

    @SuppressWarnings("FinalParameters")
    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case MessageAcknowledgementDecoder.TEMPLATE_ID:
            {
                messageAcknowledgement.wrap(buffer, offset, blockLength, version);
                return handler.onMessageAcknowledgement(
                    messageAcknowledgement.newAckedPosition(),
                    messageAcknowledgement.nodeId(),
                    messageAcknowledgement.status());
            }

            case RequestVoteDecoder.TEMPLATE_ID:
            {
                requestVote.wrap(buffer, offset, blockLength, version);
                return handler.onRequestVote(
                    requestVote.candidateId(),
                    requestVote.candidateSessionId(),
                    requestVote.leaderShipTerm(),
                    requestVote.lastAckedPosition());
            }

            case ReplyVoteDecoder.TEMPLATE_ID:
            {
                replyVote.wrap(buffer, offset, blockLength, version);
                final int nodeStateLength = replyVote.nodeStateLength();
                nodeStateBuffer.checkLimit(nodeStateLength);
                replyVote.getNodeState(nodeStateBuffer, 0, nodeStateLength);
                return handler.onReplyVote(
                    replyVote.senderNodeId(),
                    replyVote.candidateId(),
                    replyVote.leaderShipTerm(),
                    replyVote.vote(),
                    nodeStateBuffer,
                    nodeStateLength,
                    header.sessionId());
            }

            case ConsensusHeartbeatDecoder.TEMPLATE_ID:
            {
                consensusHeartbeat.wrap(buffer, offset, blockLength, version);
                return handler.onConsensusHeartbeat(
                    consensusHeartbeat.nodeId(),
                    consensusHeartbeat.leaderShipTerm(),
                    consensusHeartbeat.position(),
                    consensusHeartbeat.transportStartPosition(),
                    consensusHeartbeat.transportPosition(),
                    consensusHeartbeat.leaderSessionId());
            }

            case ResendDecoder.TEMPLATE_ID:
            {
                resend.wrap(buffer, offset, blockLength, version);
                final int bodyOffset = bodyOffset(offset, blockLength);
                return handler.onResend(
                    resend.leaderSessionId(),
                    resend.leaderShipTerm(),
                    resend.startPosition(),
                    buffer,
                    bodyOffset,
                    resend.bodyLength());
            }
        }

        return CONTINUE;
    }

    static int bodyOffset(final int offset, final int blockLength)
    {
        return offset + ENCODED_LENGTH + blockLength + bodyHeaderLength();
    }
}
