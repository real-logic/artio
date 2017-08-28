/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.stress;

import io.aeron.driver.MediaDriver;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;

import static io.aeron.driver.ThreadingMode.SHARED;
import static java.util.Collections.singletonList;
import static uk.co.real_logic.fix_gateway.SampleUtil.blockingConnect;

public class Server implements Agent
{
    private MediaDriver mediaDriver;
    private FixEngine fixEngine;
    private FixLibrary fixLibrary;

    public Server()
    {
        final AuthenticationStrategy authenticationStrategy = logon -> true;

        // Static configuration lasts the duration of a FIX-Gateway instance
        final String aeronChannel = "aeron:udp?endpoint=localhost:10000";
        final EngineConfiguration configuration = new EngineConfiguration()
            .bindTo("localhost", StressConfiguration.PORT)
            .logFileDir("stress-server-logs")
            .libraryAeronChannel(aeronChannel);
        configuration
            .authenticationStrategy(authenticationStrategy)
            .agentNamePrefix("server-");

        System.out.println("Server Logs at " + configuration.logFileDir());

        StressUtil.cleanupOldLogFileDir(configuration);

        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirDeleteOnStart(true);
        mediaDriver = MediaDriver.launch(context);
        fixEngine = FixEngine.launch(configuration);

        final LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
        libraryConfiguration
            .authenticationStrategy(authenticationStrategy)
            .agentNamePrefix("server-");
        fixLibrary = blockingConnect(libraryConfiguration
            .sessionAcquireHandler(StressSessionHandler::new)
            .sessionExistsHandler(new AcquiringSessionExistsHandler())
            .libraryAeronChannels(singletonList(aeronChannel)));
    }

    public static AgentRunner createServer(final IdleStrategy idleStrategy, final ErrorHandler errorHandler)
    {
        return new AgentRunner(idleStrategy, errorHandler, null, new Server());
    }

    public int doWork() throws Exception
    {
        return fixLibrary.poll(1);
    }

    public String roleName()
    {
        return "stress server";
    }

    public void onClose()
    {
        fixLibrary.close();
        fixEngine.close();
        mediaDriver.close();
    }
}
