package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.rest.dto.HrefDecorator;
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

import static com.dematic.labs.business.SecurityManager.ROLE_RESOURCE_PATH;

@RequestScoped
@Path(ROLE_RESOURCE_PATH)
public class RoleResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed({ApplicationRole.ADMINISTER_USERS, ApplicationRole.ADMINISTER_ROLES})
    public List<RoleDto> getList() {
        return securityManager.getRoles()
                .stream().map(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath())).collect(Collectors.toList());
    }

    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed(ApplicationRole.ADMINISTER_ROLES)
    public Response create(RoleDto userDto) {
        RoleDto returnedRoleDto = securityManager.createRole(userDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedRoleDto.getId()).build())
                .entity(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath()).apply(returnedRoleDto)).build();
    }

    @DELETE
    @Path("{id}")
    @RolesAllowed(ApplicationRole.ADMINISTER_ROLES)
    public Response delete(@PathParam("id") String id) {
        securityManager.deleteRole(id);
        return Response.noContent().build();
    }

}
