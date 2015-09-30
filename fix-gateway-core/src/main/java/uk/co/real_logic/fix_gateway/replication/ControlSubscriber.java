/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.*;

public class ControlSubscriber implements FragmentHandler
{

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final MessageAcknowledgementDecoder messageAcknowledgement = new MessageAcknowledgementDecoder();
    private final RequestVoteDecoder requestVote = new RequestVoteDecoder();
    private final ConcensusHeartbeatDecoder concensusHeartbeat = new ConcensusHeartbeatDecoder();

    private final ControlHandler handler;

    public ControlSubscriber(final ControlHandler handler)
    {
        this.handler = handler;
    }

    public void onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case MessageAcknowledgementEncoder.TEMPLATE_ID:
            {

                messageAcknowledgement.wrap(buffer, offset, blockLength, version);
                handler.onMessageAcknowledgement(
                    messageAcknowledgement.newAckedPosition(),
                    messageAcknowledgement.nodeId()
                );
                return;
            }

            case RequestVoteEncoder.TEMPLATE_ID:
            {
                requestVote.wrap(buffer, offset, blockLength, version);
                handler.onRequestVote(
                    requestVote.candidateId(),
                    requestVote.lastAckedPosition()
                );
                return;
            }

            case ConcensusHeartbeatEncoder.TEMPLATE_ID:
            {
                concensusHeartbeat.wrap(buffer, offset, blockLength, version);
                handler.onConcensusHeartbeat(
                    concensusHeartbeat.nodeId()
                );
                return;
            }
        }
    }
}
