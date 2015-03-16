package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.rest.dto.HrefDecorator;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityManager.TENANT_RESOURCE_PATH;

@RequestScoped
@Path(TENANT_RESOURCE_PATH)
public class TenantResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerTenants")
    public List<TenantDto> getList() {
        return securityManager.getTenants()
                .stream().map(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath())).collect(Collectors.toList());
    }


    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerTenants")
    public Response create(TenantDto tenantDto) {
        TenantDto returnedTenantDto = securityManager.createTenant(tenantDto);
        URI uri = uriInfo.getBaseUriBuilder().path(returnedTenantDto.getId()).build();
        return Response.created(uri)
                .entity(new HrefDecorator<>(uriInfo.getAbsolutePath().getPath()).apply(returnedTenantDto)).build();
    }

    @DELETE
    @Path("{id}")
    @RolesAllowed("administerTenants")
    public Response delete(@PathParam("id") String id) {
        securityManager.deleteTenant(id);
        return Response.noContent().build();
    }

}
