/*
 * Copyright 2015-2021 Real Logic Limited.
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
package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.fixp.FixPIdentification;
import uk.co.real_logic.artio.validation.FixPAuthenticationProxy;
import uk.co.real_logic.artio.validation.FixPAuthenticationStrategy;

public class FakeFixPAuthenticationStrategy implements FixPAuthenticationStrategy
{
    private volatile boolean accept;
    private volatile FixPIdentification lastSessionId;

    public FakeFixPAuthenticationStrategy()
    {
        accept();
    }

    public void accept()
    {
        this.accept = true;
    }

    public void reject()
    {
        this.accept = false;
    }

    public void authenticate(final FixPIdentification sessionId, final FixPAuthenticationProxy authProxy)
    {
        lastSessionId = sessionId;
        if (accept)
        {
            authProxy.accept();
        }
        else
        {
            authProxy.reject();
        }
    }

    public FixPIdentification lastSessionId()
    {
        return lastSessionId;
    }
}
