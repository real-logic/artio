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
package uk.co.real_logic.fix_gateway.timing;

import java.util.Arrays;
import java.util.List;

public class LibraryTimers
{
    private final Timer sessionTimer = new Timer("Session", -1);
    private final Timer receiveTimer = new Timer("Receive", -2);
    private final List<Timer> timers = Arrays.asList(sessionTimer, receiveTimer);

    public Timer sessionTimer()
    {
        return sessionTimer;
    }

    public Timer receiveTimer()
    {
        return receiveTimer;
    }

    public List<Timer> all()
    {
        return timers;
    }
}
