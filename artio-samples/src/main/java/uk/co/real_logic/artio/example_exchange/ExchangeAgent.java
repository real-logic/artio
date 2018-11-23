package uk.co.real_logic.artio.example_exchange;

import org.agrona.concurrent.Agent;
import uk.co.real_logic.artio.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.util.Collections;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static uk.co.real_logic.artio.example_exchange.ExchangeApplication.ACCEPTOR_COMP_ID;
import static uk.co.real_logic.artio.example_exchange.ExchangeApplication.INITIATOR_COMP_ID;

public class ExchangeAgent implements Agent
{
    private static final int FRAGMENT_LIMIT = 10;

    private final LibraryConfiguration configuration = new LibraryConfiguration();
    private FixLibrary library;

    public ExchangeAgent()
    {
    }

    @Override
    public void onStart()
    {
        final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId(ACCEPTOR_COMP_ID)
            .and(MessageValidationStrategy.senderCompId(Collections.singletonList(INITIATOR_COMP_ID)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        configuration.authenticationStrategy(authenticationStrategy);

        // You register the new session handler - which is your application hook
        // that receives messages for new sessions
        configuration
            .sessionAcquireHandler(this::onAcquire)
            .sessionExistsHandler(new AcquiringSessionExistsHandler())
            .libraryAeronChannels(singletonList(IPC_CHANNEL));

        library = FixLibrary.connect(configuration);
    }

    private SessionHandler onAcquire(final Session session, final boolean isSlow)
    {
        return new ExchangeSessionHandler(session);
    }

    @Override
    public int doWork()
    {
        return library.poll(FRAGMENT_LIMIT);
    }

    @Override
    public String roleName()
    {
        return "Exchange";
    }
}
