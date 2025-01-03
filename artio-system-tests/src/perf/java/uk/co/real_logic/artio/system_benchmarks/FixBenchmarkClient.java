/*
 * Copyright 2015-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_benchmarks;

import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.TYPE;

public final class FixBenchmarkClient
{
    public static void main(final String[] args) throws Exception
    {
        if (TYPE.equalsIgnoreCase("throughput"))
        {
            ThroughputBenchmarkClient.main(args);
        }
        else if (TYPE.equalsIgnoreCase("many-connections"))
        {
            ManyConnectionsBenchmarkClient.main(args);
        }
        else if (TYPE.equalsIgnoreCase("repeat-connections"))
        {
            RepeatConnectionBenchmarkClient.main(args);
        }
        else
        {
            LatencyBenchmarkClient.main(args);
        }
    }
}
