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
package uk.co.real_logic.fix_gateway.builder;

import sun.nio.ch.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.flyweight_api.StandardHeader;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

/**
 * .
 */
public class LogonEncoder
{

    private final MutableDirectBuffer buffer;

    StandardHeader standardHeader;
    int heartBeatInterval;
    // Or offer a default value
    int rawDataLength;
    DirectBuffer rawData;
    boolean resetSeqNumFlag;
    int maxMessageSize;
    int noMsgTypes;

    boolean hasRefMsgType;
    AsciiFlyweight refMsgType;

    char msgDirection;

    LogonEncoder(final MutableDirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    public int encode(final int offset)
    {
        return 0;
    }

}
