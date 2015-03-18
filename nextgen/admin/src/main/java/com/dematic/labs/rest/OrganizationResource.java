package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.OrganizationManager;
import com.dematic.labs.business.Pagination;
import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.rest.dto.HrefDecorator;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.dematic.labs.business.OrganizationManager.ORGANIZATION_RESOURCE_PATH;

@RequestScoped
@Path(ORGANIZATION_RESOURCE_PATH)
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
                .entity(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath()).apply(returnedOrganizationDto)).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("{id}")
    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public Response getOrganization(@PathParam("id") String id) {
        if (id == null) {
            throw new IllegalArgumentException("Must supply id when getting a single organization");
        }
        OrganizationDto returnedOrganizationDto = organizationManager.getOrganization(UUID.fromString(id));
        return Response.ok(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath()).apply(returnedOrganizationDto))
                .build();
    }

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public CollectionDto<OrganizationDto> getList(@DefaultValue("0") @QueryParam("offset") int offset,
                                                  @DefaultValue("25") @QueryParam("limit") int limit) {

        CollectionDto<OrganizationDto> collectionDto = organizationManager
                .getOrganizations(new Pagination(offset, limit));
        collectionDto.getItems().stream()
                .map(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath()))
                .collect(Collectors.toList());
        return collectionDto;
    }

    @PUT
    @Path("{id}/grant")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed(ApplicationRole.ADMINISTER_ORGANIZATION_BUSINESS_ROLES)
    public Response grant(@PathParam("id") String id, OrganizationDto organizationDto) {
        String rawPath = uriInfo.getAbsolutePath().getPath();
        String cookedPath = rawPath.substring(0, rawPath.indexOf(id)-1);
        OrganizationDto returnedOrganizationDto = organizationManager.grantRevokeBusinessRole(organizationDto);
        return Response.ok(new HrefDecorator<>(cookedPath).apply(returnedOrganizationDto)).build();
    }

}
