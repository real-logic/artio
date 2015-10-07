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

import uk.co.real_logic.aeron.driver.LossGenerator;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

public class SwitchableLossGenerator implements LossGenerator
{
    private volatile boolean dropFrames = false;

    public void dropFrames(final boolean dropFrames)
    {
        this.dropFrames = dropFrames;
    }

    public boolean shouldDropFrame(
        final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        return dropFrames;
    }
}
