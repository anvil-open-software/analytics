package com.dematic.labs.rest;

import com.dematic.labs.rest.dto.RestError;
import org.apache.deltaspike.security.api.authorization.AccessDeniedException;

import javax.enterprise.context.ApplicationScoped;
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
   }

    @Override
    public Response toResponse(Throwable exception) {

        for (Throwable throwable = exception; throwable != null; throwable = throwable.getCause()) {
            Class<? extends Throwable> throwableClass = throwable.getClass();
            Response.StatusType status = exceptionMapping.get(throwableClass);

            if (status != null) {
                return Response.status(status).entity(new RestError(status, throwable.getMessage())).build();
            }
        }

        throw new UnhandledException(exception);
    }

}
