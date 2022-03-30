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

import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;

/**
 * Customer interface to control whether resend requests are responded to or not.
 *
 * There is also the related {@link uk.co.real_logic.artio.engine.ReplayHandler} which allows you to see which messages
 * are resent.
 */
public interface ResendRequestController
{
    /**
     * Callback can be implemented by the application in order to control the response to a resend request.
     *
     * Note: this callback is only called if the resend request is believed to be valid. If the resend request message
     * itself is invalid (eg: sequence number out of order or sending time out of range) or if the range is invalid
     * (eg: begin sequence number &gt; end sequence number or begin sequence number &gt; last sent sequence number)
     * then this callback won't be invoked.
     *
     * @param session the session that has received the resend request.
     * @param resendRequest the decoded resend request in question.
     * @param correctedEndSeqNo the end sequence number that Artio will reply with. This is useful if, for example, the
     *                          resend request uses 0 for its endSeqNo parameter.
     * @param response respond to the resend request by calling methods on this object.
     */
    void onResend(
        Session session,
        AbstractResendRequestDecoder resendRequest,
        int correctedEndSeqNo,
        ResendRequestResponse response);

    /**
     * This method is invoked when a Session identifies that a resend is complete. It is invoked on the thread
     * that the Library is polled on that owns the Session in question.
     *
     * @param session the session on which resend is complete.
     * @param remainingReplaysInFlight the number of remaining replays in flight for this Session after the completion of this
     *                        replay.
     */
    default void onResendComplete(Session session, final int remainingReplaysInFlight)
    {
        // default and empty for backwards compatibility reasons.
    }
}
