package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.UserDto;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.stream.Collectors;

@RequestScoped
@Path("user")
public class UserResource extends HrefResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerUsers")
    public List<UserDto> getList() {
        return securityManager.getUsers()
                .stream().map(new HrefDecorator<>(uriInfo.getBaseUri().toString())).collect(Collectors.toList());
    }

    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerUsers")
    public Response create(UserDto userDto) {
        UserDto returnedTenantDto = securityManager.createTenantUser(userDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedTenantDto.getId()).build())
                .entity(new HrefDecorator<>(uriInfo.getBaseUri().toString()).apply(returnedTenantDto)).build();
    }

    @PUT
    @Path("{id}/grant")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerUsers")
    public Response grant(@PathParam("id") String id, UserDto userDto) {
        return Response.ok(new HrefDecorator<>(uriInfo.getBaseUri().toString()).apply(securityManager.grantRevokeUserRole(userDto))).build();
    }

}
