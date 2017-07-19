package uk.co.real_logic.fix_gateway.engine.framer;

class AuthenticationResult {
    static final AuthenticationResult DUPLICATE_SESSION = new AuthenticationResult(AuthenticationError.DUPLICATE_SESSION);
    static final AuthenticationResult FAILED_AUTHENTICATION = new AuthenticationResult(AuthenticationError.FAILED_AUTHENTICATION);

    enum AuthenticationError { DUPLICATE_SESSION, FAILED_AUTHENTICATION }

    final GatewaySession session;
    final int sentSequenceNumber;
    final int receivedSequenceNumber;
    final AuthenticationError error;

    private AuthenticationResult(AuthenticationError error)
    {
        this.session = null;
        this.error = error;
        sentSequenceNumber = -1;
        receivedSequenceNumber = -1;
    }

    private AuthenticationResult(GatewaySession session, int sentSequenceNumber, int receivedSequenceNumber)
    {
        this.session = session;
        this.sentSequenceNumber = sentSequenceNumber;
        this.receivedSequenceNumber = receivedSequenceNumber;
        this.error = null;
    }

    static AuthenticationResult authenticatedSession(GatewaySession session, int sentSequenceNumber, int receivedSequenceNumber)
    {
        return new AuthenticationResult(session, sentSequenceNumber, receivedSequenceNumber);
    }

    boolean isDuplicateSession()
    {
        return null != error && AuthenticationError.DUPLICATE_SESSION == error;
    }

    boolean isValid() {
        return null != session;
    }
}
