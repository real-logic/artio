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

import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

public interface ControlHandler
{
    void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status);

    void onRequestVote(final short candidateId, final int leaderShipTerm, final long lastAckedPosition);

    void onReplyVote(final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote);

    void onConcensusHeartbeat(final short nodeId, final int leaderShipTerm, final long position);
}
