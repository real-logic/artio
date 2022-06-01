/*
 * Copyright 2019 Monotonic Ltd.
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
package uk.co.real_logic.artio.decoder;

import org.agrona.AsciiSequenceView;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.util.AsciiBuffer;

public interface SessionHeaderDecoder
{
    int decode(AsciiBuffer buffer, int offset, int length);

    /**
     * Resets the result of the decoder.
     */
    void reset();

    StringBuilder appendTo(StringBuilder builder);

    StringBuilder appendTo(StringBuilder builder, int level);

    SessionHeaderEncoder toEncoder(Encoder encoder);

    char[] beginString();

    int beginStringLength();

    int msgSeqNum();

    byte[] origSendingTime();

    boolean hasOrigSendingTime();

    int origSendingTimeLength();

    String origSendingTimeAsString();

    AsciiSequenceView origSendingTime(AsciiSequenceView view);

    int msgTypeLength();

    char[] msgType();

    /**
     * Gets the packed message type from the decoded message.
     *
     * @return packed message type.
     */
    long messageType();

    boolean hasPossResend();

    boolean possResend();

    boolean possDupFlag();

    boolean hasPossDupFlag();

    byte[] sendingTime();

    int sendingTimeLength();

    String sendingTimeAsString();

    AsciiSequenceView sendingTime(AsciiSequenceView view);

    char[] senderCompID();

    int senderCompIDLength();

    char[] senderSubID();

    int senderSubIDLength();

    char[] senderLocationID();

    int senderLocationIDLength();

    char[] targetCompID();

    int targetCompIDLength();

    char[] targetSubID();

    int targetSubIDLength();

    char[] targetLocationID();

    int targetLocationIDLength();

    boolean hasSenderLocationID();

    boolean hasSenderSubID();

    boolean hasTargetLocationID();

    boolean hasTargetSubID();

    String senderCompIDAsString();

    String senderSubIDAsString();

    String senderLocationIDAsString();

    String targetCompIDAsString();

    String targetSubIDAsString();

    String targetLocationIDAsString();

    AsciiSequenceView senderCompID(AsciiSequenceView view);

    AsciiSequenceView senderSubID(AsciiSequenceView view);

    AsciiSequenceView senderLocationID(AsciiSequenceView view);

    AsciiSequenceView targetCompID(AsciiSequenceView view);

    AsciiSequenceView targetSubID(AsciiSequenceView view);

    AsciiSequenceView targetLocationID(AsciiSequenceView view);
}
