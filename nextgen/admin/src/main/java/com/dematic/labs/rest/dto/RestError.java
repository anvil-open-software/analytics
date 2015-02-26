package com.dematic.labs.rest.dto;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RestError {

    private int httpStatusCode;
    private String httpStatus;
    private String message;

    public RestError(@Nonnull Response.StatusType status, @Nullable String message) {
        this.message = message;
        httpStatusCode = status.getStatusCode();
        httpStatus = status.getReasonPhrase();
    }

    @SuppressWarnings("UnusedDeclaration")
    public RestError() {
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public void setHttpStatusCode(int httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public String getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(String httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
