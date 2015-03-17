package com.dematic.labs.rest;

import com.dematic.labs.rest.dto.RestError;
import org.apache.deltaspike.security.api.authorization.AccessDeniedException;
import org.picketlink.idm.IdentityManagementException;

import javax.enterprise.context.ApplicationScoped;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;

@Provider
@ApplicationScoped
public class RestExceptionMapper implements ExceptionMapper<Throwable> {

    Map<Class<? extends Throwable>, Response.StatusType> exceptionMapping = new HashMap<>();

    public RestExceptionMapper() {

        exceptionMapping.put(AccessDeniedException.class, Response.Status.FORBIDDEN);
        exceptionMapping.put(IllegalArgumentException.class, Response.Status.BAD_REQUEST);
        exceptionMapping.put(IllegalStateException.class, Response.Status.BAD_REQUEST);
        exceptionMapping.put(IdentityManagementException.class, Response.Status.BAD_REQUEST);
        exceptionMapping.put(ConstraintViolationException.class, Response.Status.BAD_REQUEST);
   }

    @Override
    public Response toResponse(Throwable exception) {

        for (Throwable throwable = exception; throwable != null; throwable = throwable.getCause()) {
            Class<? extends Throwable> throwableClass = throwable.getClass();
            Response.StatusType status = exceptionMapping.get(throwableClass);

            if (status != null) {
                RestError restError;
                if (throwable instanceof ConstraintViolationException) {
                    restError = new RestError(status, (ConstraintViolationException) throwable);
                } else {
                    restError = new RestError(status, throwable.getMessage());
                }

                return Response.status(status).entity(restError).build();
            }
        }

        throw new UnhandledException(exception);
    }

}
