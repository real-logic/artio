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
import uk.co.real_logic.artio.util.AsciiBuffer;

public interface SessionHeaderDecoder
{
    int decode(AsciiBuffer buffer, int offset, int length);

    /**
     * Resets the result of the decoder.
     */
    void reset();

    char[] beginString();

    int beginStringLength();

    int msgSeqNum();

    byte[] origSendingTime();

    boolean hasOrigSendingTime();

    int origSendingTimeLength();

    String origSendingTimeAsString();

    void origSendingTime(AsciiSequenceView view);

    int msgTypeLength();

    char[] msgType();

    boolean hasPossResend();

    boolean possResend();

    boolean possDupFlag();

    boolean hasPossDupFlag();

    byte[] sendingTime();

    int sendingTimeLength();

    String sendingTimeAsString();

    void sendingTime(AsciiSequenceView view);

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

    void senderCompID(AsciiSequenceView view);

    void senderSubID(AsciiSequenceView view);

    void senderLocationID(AsciiSequenceView view);

    void targetCompID(AsciiSequenceView view);

    void targetSubID(AsciiSequenceView view);

    void targetLocationID(AsciiSequenceView view);
}
