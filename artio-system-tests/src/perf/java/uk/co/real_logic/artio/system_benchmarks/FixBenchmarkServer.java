/*
 * Copyright 2015-2022 Real Logic Limited.
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

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.builder.LogoutEncoder;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.LibraryConnectHandler;
import uk.co.real_logic.artio.validation.AuthenticationProxy;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.*;

public final class FixBenchmarkServer
{
    public static void main(final String[] args)
    {
        final EngineConfiguration configuration = engineConfiguration();

        try (MediaDriver mediaDriver = newMediaDriver();
            FixEngine engine = FixEngine.launch(configuration);
            FixLibrary library = FixLibrary.connect(libraryConfiguration()))
        {
            final IdleStrategy idleStrategy = idleStrategy();
            System.out.printf("Using %s idle strategy%n", idleStrategy.getClass().getSimpleName());
            while (true)
            {
                final boolean notConnected = !library.isConnected();

                idleStrategy.idle(library.poll(10));

                if (notConnected && library.isConnected())
                {
                    break;
                }
            }

            while (true)
            {
                idleStrategy.idle(library.poll(10));
            }
        }
    }

    private static MediaDriver newMediaDriver()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .publicationTermBufferLength(128 * 1024 * 1024);

        return MediaDriver.launch(context);
    }

    private static EngineConfiguration engineConfiguration()
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        configuration.printAeronStreamIdentifiers(true);

        configuration.authenticationStrategy(new BenchmarkAuthenticationStrategy());

        return configuration
            .bindTo("localhost", BenchmarkConfiguration.PORT)
            .libraryAeronChannel(AERON_CHANNEL)
            .deleteLogFileDirOnStart(true)
            .logFileDir("acceptor_logs")
            .logInboundMessages(LOG_INBOUND_MESSAGES)
            .logOutboundMessages(LOG_OUTBOUND_MESSAGES)
            .framerIdleStrategy(idleStrategy());
    }

    private static LibraryConfiguration libraryConfiguration()
    {
        final LibraryConfiguration configuration = new LibraryConfiguration();
        configuration.printAeronStreamIdentifiers(true);

        return configuration
            .libraryAeronChannels(singletonList(AERON_CHANNEL))
            .sessionAcquireHandler((session, acquiredInfo) -> new BenchmarkSessionHandler())
            .sessionExistsHandler(new AcquiringSessionExistsHandler(true))
            .libraryConnectHandler(new LibraryConnectHandler()
            {
                public void onConnect(final FixLibrary library)
                {
                    System.out.println("Library: onConnect");
                }

                public void onDisconnect(final FixLibrary library)
                {
                    System.out.println("Library: onDisconnect");
                }
            });
    }

    private static class BenchmarkAuthenticationStrategy implements AuthenticationStrategy
    {
        private static final byte[] INVALID_PASSWORD = "Invalid Password".getBytes(StandardCharsets.US_ASCII);
        private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

        public void authenticateAsync(final AbstractLogonDecoder logon, final AuthenticationProxy authProxy)
        {
            final int length = VALID_PASSWORD_CHARS.length;
            final boolean validPassword = logon.hasPassword() &&
                logon.passwordLength() == length &&
                CodecUtil.equals(logon.password(), VALID_PASSWORD_CHARS, length);

            // Simulate the delay of talking to a logon service
            scheduledExecutorService.schedule(() ->
            {
                if (validPassword)
                {
                    authProxy.accept();
                }
                else
                {
                    final LogoutEncoder logout = new LogoutEncoder();
                    logout.text(INVALID_PASSWORD);
                    authProxy.reject(logout, LOGOUT_LINGER_TIMEOUT_IN_MS);
                }
            }, 20L, TimeUnit.MILLISECONDS);
        }

        public boolean authenticate(final AbstractLogonDecoder logon)
        {
            // Unused
            return true;
        }
    }
}
