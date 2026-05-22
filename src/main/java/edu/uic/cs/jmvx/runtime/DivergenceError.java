package edu.uic.cs.jmvx.runtime;

public class DivergenceError extends Error {

    public DivergenceError() {
    }

    public DivergenceError(String message) {
        super(message);
    }

    public DivergenceError(String message, Throwable cause) {
        super(message, cause);
    }

    public DivergenceError(Throwable cause) {
        super(cause);
    }
}
