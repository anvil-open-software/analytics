package com.dematic.labs.rest;

public class UnhandledException extends RuntimeException {
    public UnhandledException(Throwable exception) {
        super(exception);
    }
}
