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
package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.session.ResendRequestController;
import uk.co.real_logic.artio.session.ResendRequestResponse;
import uk.co.real_logic.artio.session.Session;

import static org.junit.Assert.assertNotNull;

public class FakeResendRequestController implements ResendRequestController
{
    private boolean resend = true;

    private boolean called = false;

    public void onResend(
        final Session session,
        final AbstractResendRequestDecoder resendRequest,
        final int correctedEndSeqNo,
        final ResendRequestResponse response)
    {
        called = true;
        assertNotNull(resendRequest);

        if (resend)
        {
            response.resend();
        }
        else
        {
            response.reject(Constants.BEGIN_SEQ_NO);
        }
    }

    public void resend(final boolean resend)
    {
        this.resend = resend;
    }

    public boolean wasCalled()
    {
        return called;
    }
}
