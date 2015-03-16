package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.rest.dto.UserDtoHrefDecorator;
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
@Path("tenantAdminUser")
public class TenantAdminUserResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerTenants")
    public List<UserDto> getList() {
        return securityManager.getTenantsAdminUsers()
                .stream().map(new UserDtoHrefDecorator(uriInfo.getAbsolutePath().getPath())).collect(Collectors.toList());
    }


    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerTenants")
    public Response create(UserDto userDto) {
        UserDto returnedTenantDto = securityManager.createTenantAdmin(userDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedTenantDto.getId()).build())
                .entity(new UserDtoHrefDecorator(uriInfo.getAbsolutePath().getPath()).apply(returnedTenantDto)).build();
    }



}
