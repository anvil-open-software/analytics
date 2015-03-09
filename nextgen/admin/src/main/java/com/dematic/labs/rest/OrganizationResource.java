package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.OrganizationManager;
import com.dematic.labs.business.dto.OrganizationDto;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.UUID;

@RequestScoped
@Path("principal")
public class OrganizationResource {

    @EJB
    OrganizationManager organizationManager;

    @Context
    UriInfo uriInfo;

    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed(ApplicationRole.CREATE_ORGANIZATIONS)
    public Response create(OrganizationDto organizationDto) {
        OrganizationDto returnedOrganizationDto = organizationManager.create(organizationDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedOrganizationDto.getId()).build())
                .entity(returnedOrganizationDto).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("{id}")
    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public OrganizationDto getOrganization(@PathParam("id") String id) {
        if (id == null) {
            return null;
        }
        return organizationManager.getOrganization(UUID.fromString(id));
    }

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public OrganizationDto[] getList() {
        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return organizationManager.getOrganizations().toArray(new OrganizationDto[0]);
    }

}
