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

import org.agrona.LangUtil;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

// NB: This is an experimental API and is subject to change or potentially removal.
public class ILink3Session extends ILink3EndpointHandler
{
    private static final long MICROS_IN_MILLIS = 1000;

    private final AbstractILink3Proxy abstractILink3Proxy;
    private final ILink3SessionConfiguration configuration;
    private final long connection;
    private final Consumer<ILink3Session> onActive;

    public enum State
    {
        CONNECTING,
        CONNECTED,
        SENT_NEGOTIATE,
        NEGOTIATE_REJECTED,
        SENT_ESTABLISH,
        ESTABLISH_REJECTED,
        ACTIVE,
        UNBINDING,
        DISCONNECTED
    }

    private final long uuid;
    private State state;

    public ILink3Session(
        final AbstractILink3Proxy abstractILink3Proxy,
        final ILink3SessionConfiguration configuration,
        final long connection,
        final Consumer<ILink3Session> onActive)
    {
        this.abstractILink3Proxy = abstractILink3Proxy;
        this.configuration = configuration;
        this.connection = connection;
        this.onActive = onActive;

        uuid = microSecondTimestamp();
        state = State.CONNECTING;

        sendNegotiate();
    }

    private long microSecondTimestamp()
    {
        final long microseconds = NANOSECONDS.toMicros(System.nanoTime()) % MICROS_IN_MILLIS;
        return MILLISECONDS.toMicros(System.currentTimeMillis()) + microseconds;
    }

    private void sendNegotiate()
    {
        final long requestTimestamp = microSecondTimestamp();
        final String sessionId = configuration.sessionId();
        final String firmId = configuration.firmId();
        final String canonicalMsg = String.valueOf(requestTimestamp) + '\n' + uuid + '\n' + sessionId + '\n' + firmId;
        final byte[] hMACSignature = calculateHMAC(canonicalMsg, configuration.userKey());

        abstractILink3Proxy.sendNegotiate(
            hMACSignature, configuration.accessKeyId(), uuid, requestTimestamp, sessionId, firmId);
    }

    public long uuid()
    {
        return uuid;
    }

    private byte[] calculateHMAC(final String canonicalRequest, final String userKey)
    {
        try
        {
            final Mac sha256HMAC = getHmac();

            // Decode the key first, since it is base64url encoded
            byte[] decodedUserKey = Base64.getUrlDecoder().decode(userKey);
            SecretKeySpec secretKey = new SecretKeySpec(decodedUserKey, "HmacSHA256");
            sha256HMAC.init(secretKey);

            // Calculate HMAC
            return sha256HMAC.doFinal(canonicalRequest.getBytes(UTF_8));
        }
        catch (final InvalidKeyException | IllegalStateException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    private Mac getHmac()
    {
        try
        {
            return Mac.getInstance("HmacSHA256");
        }
        catch (final NoSuchAlgorithmException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }
}
