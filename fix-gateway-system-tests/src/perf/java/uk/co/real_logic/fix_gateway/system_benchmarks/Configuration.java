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
package uk.co.real_logic.fix_gateway.system_benchmarks;

import static uk.co.real_logic.aeron.CommonContext.IPC_CHANNEL;

public final class Configuration
{
    public static final int PORT = Integer.getInteger("fix.benchmark.port", 9999);
    public static final String AERON_CHANNEL = System.getProperty("fix.benchmark.aeron_channel", IPC_CHANNEL);
    public static final String ACCEPTOR_ID = "ACC";
    public static final String INITIATOR_ID = "INIT";
    public static final String TYPE = System.getProperty("fix.benchmark.type", "latency");

}
