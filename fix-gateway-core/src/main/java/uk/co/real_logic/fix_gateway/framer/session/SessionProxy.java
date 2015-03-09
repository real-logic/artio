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
package uk.co.real_logic.fix_gateway.framer.session;

import uk.co.real_logic.fix_gateway.builder.*;

/**
 * Encapsulates sending messages relating to sessions
 */
// TODO: conversion from session id to sender id/comp id
// TODO: buffer
public class SessionProxy
{
    private final LogonEncoder logon = new LogonEncoder();
    private final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();
    private final HeartbeatEncoder heartbeat = new HeartbeatEncoder();
    private final RejectEncoder reject = new RejectEncoder();

    public void resendRequest(final int beginSeqNo, final int endSeqNo)
    {
        resendRequest.beginSeqNo(beginSeqNo)
                     .endSeqNo(endSeqNo);
    }

    public void disconnect(final long connectionId)
    {

    }

    public void logon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        logon.header().msgSeqNum(msgSeqNo);
        logon.heartBtInt(heartbeatInterval);
    }

    public void logout(final int msgSeqNo, final long sessionId)
    {
        logout.header().msgSeqNum(msgSeqNo);
    }

    public void heartbeat(final String testReqId)
    {
        heartbeat.testReqID(testReqId);
    }

    public void reject(final int msgSeqNo, final int refSeqNum)
    {
        reject.header().msgSeqNum(msgSeqNo);
        reject.refSeqNum(refSeqNum);
        // TODO: decide on other ref fields
    }
}
