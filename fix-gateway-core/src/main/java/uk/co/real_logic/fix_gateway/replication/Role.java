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

/**
 * Replication role determines your actions.
 */
public enum Role
{
    /**
     * Responsible for committing writes once a majority of followers acknowledge them.
     */
    LEADER,

    /**
     * Acknowledge writes.
     */
    FOLLOWER,

    /**
     * A node in the process of a leader election, trying to become a Leader.
     */
    CANDIDATE

    // TODO: do we need another role for a Follower that is purely a log/ack server?
}
