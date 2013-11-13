package com.opennms.lucidity;


public class LucidityException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public LucidityException() {
        super();
    }

    public LucidityException(String message) {
        super(message);
    }

    public LucidityException(Throwable throwable) {
        super(throwable);
    }

    public LucidityException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
