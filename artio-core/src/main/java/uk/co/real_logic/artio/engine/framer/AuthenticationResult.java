package uk.co.real_logic.artio.engine.framer;

final class AuthenticationResult
{
    static final AuthenticationResult DUPLICATE_SESSION =
        new AuthenticationResult(AuthenticationError.DUPLICATE_SESSION);
    static final AuthenticationResult FAILED_AUTHENTICATION =
        new AuthenticationResult(AuthenticationError.FAILED_AUTHENTICATION);

    enum AuthenticationError
    {
        DUPLICATE_SESSION, FAILED_AUTHENTICATION
    }

    final GatewaySession session;
    final AuthenticationError error;

    private AuthenticationResult(final AuthenticationError error)
    {
        this.session = null;
        this.error = error;
    }

    private AuthenticationResult(
        final GatewaySession session)
    {
        this.session = session;
        this.error = null;
    }

    static AuthenticationResult authenticatedSession(
        final GatewaySession session)
    {
        return new AuthenticationResult(session);
    }

    boolean isDuplicateSession()
    {
        return null != error && AuthenticationError.DUPLICATE_SESSION == error;
    }

    boolean isValid()
    {
        return null != session;
    }
}
