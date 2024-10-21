/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.session;

import uk.co.real_logic.artio.builder.AbstractRejectEncoder;
import uk.co.real_logic.artio.util.AsciiBuffer;

public class ResendRequestResponse
{
    private boolean resendNow;
    private boolean delayProcessing;

    private int refTagId;
    private AbstractRejectEncoder rejectEncoder;

    /**
     * Invoke when you want to apply normal behaviour and respond to the resend request.
     */
    public void resend()
    {
        resendNow = true;
        delayProcessing = false;
    }

    /**
     * Invoke when you want to reject the resend request.
     *
     * @param refTagId the tag id that has motivated the reject.
     */
    public void reject(final int refTagId)
    {
        this.refTagId = refTagId;

        resendNow = false;
        delayProcessing = false;
    }

    public void reject(final AbstractRejectEncoder rejectEncoder)
    {
        this.rejectEncoder = rejectEncoder;

        resendNow = false;
        delayProcessing = false;
    }

    AbstractRejectEncoder rejectEncoder()
    {
        return rejectEncoder;
    }

    boolean result()
    {
        return resendNow;
    }

    int refTagId()
    {
        return refTagId;
    }

    /**
     * Since version 0.148(?) it is possible to postpone the execution of a ResendRequest. This method indicates
     * that the request must not be processed nor rejected. It is the responsibility of the caller to call
     * Session.executeResendRequest() when ready.
     *
     * @see Session#executeResendRequest(int, int, AsciiBuffer, int, int)
     * @return true if response to the request must not be done immediately
     */
    public boolean shouldDelay()
    {
        return delayProcessing;
    }

    /**
     * This method indicates that the request must not be processed nor rejected. It is the responsibility of
     * the caller to call Session.executeResendRequest() when ready.
     *
     * @see Session#executeResendRequest(int, int, AsciiBuffer, int, int)
     */
    public void delay()
    {
        resendNow = false;
        delayProcessing = true;
    }
}
