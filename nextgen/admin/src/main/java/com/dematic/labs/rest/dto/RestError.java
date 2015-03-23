package com.dematic.labs.rest.dto;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@XmlRootElement
public class RestError {

    private int httpStatusCode;
    private String httpStatus;
    private String message;

    private Set<String> constraintViolationMessages = new HashSet<>();

    public RestError(@Nonnull Response.StatusType status, @Nullable String message) {
        this.message = message;
        httpStatusCode = status.getStatusCode();
        httpStatus = status.getReasonPhrase();
    }

    public RestError(@Nonnull Response.StatusType status, @Nonnull ConstraintViolationException cve) {
        httpStatusCode = status.getStatusCode();
        httpStatus = status.getReasonPhrase();

        cve.getConstraintViolations().stream().forEach(p->constraintViolationMessages.add(p.getMessage()));

    }

    @SuppressWarnings("UnusedDeclaration")
    public RestError() {
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public String getHttpStatus() {
        return httpStatus;
    }

    public String getMessage() {
        return message;
    }

    public Set<String> getConstraintViolationMessages() {
        return Collections.unmodifiableSet(constraintViolationMessages);
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setHttpStatusCode(int httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setHttpStatus(String httpStatus) {
        this.httpStatus = httpStatus;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setMessage(String message) {
        this.message = message;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setConstraintViolationMessages(Set<String> constraintViolationMessages) {
        this.constraintViolationMessages = constraintViolationMessages;
    }

}
