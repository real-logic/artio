/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.ilink;

public abstract class AbstractILink3Proxy
{

    public static final int ARTIO_HEADER_LENGTH = 16;

    public abstract long sendNegotiate(
        byte[] hMACSignature,
        String accessKeyId,
        long uuid,
        long requestTimestamp,
        String sessionId,
        String firmId);

    public abstract long sendEstablish(
        byte[] hMACSignature,
        String accessKeyId,
        String tradingSystemName,
        String tradingSystemVendor,
        String tradingSystemVersion,
        long uuid,
        long requestTimestamp,
        int nextSentSeqNo,
        String sessionId,
        String firmId,
        int keepAliveInterval);
}
