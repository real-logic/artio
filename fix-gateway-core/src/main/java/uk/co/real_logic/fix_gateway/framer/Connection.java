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
package uk.co.real_logic.fix_gateway.framer;

import java.net.InetSocketAddress;

/**
 * ThreadSafe for publication, operations delegating to receiver and sender end points
 * should be
 */
public class Connection
{
    private final InetSocketAddress remoteAddress;
    private final ReceiverEndPoint receiverEndPoint;
    private final SenderEndPoint senderEndPoint;

    public Connection(
            final InetSocketAddress remoteAddress,
            final ReceiverEndPoint receiverEndPoint,
            final SenderEndPoint senderEndPoint)
    {
        this.remoteAddress = remoteAddress;
        this.receiverEndPoint = receiverEndPoint;
        this.senderEndPoint = senderEndPoint;
    }

    public ReceiverEndPoint receiverEndPoint()
    {
        return this.receiverEndPoint;
    }

    public SenderEndPoint senderEndPoint()
    {
        return this.senderEndPoint;
    }

}
