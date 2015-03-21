package com.dematic.labs.rest;

import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.rest.dto.UserDtoUriDecorator;
import com.dematic.labs.rest.helpers.OrderByQueryParameterConverter;
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
    public CollectionDto<UserDto> getList(@DefaultValue("0") @QueryParam("offset") int offset,
                                          @DefaultValue(QueryParameters.DEFAULT_LIMIT_AS_STRING) @QueryParam("limit") int limit,
                                          @QueryParam("orderBy") String orderByClause) {
        List<QueryParameters.ColumnSort> orderBy = OrderByQueryParameterConverter.convert(orderByClause);

        CollectionDto<UserDto> collectionDto = securityManager.getTenantsAdminUsers(new QueryParameters(offset, limit, orderBy));
        collectionDto.getItems()
                .stream()
                .map(new UserDtoUriDecorator(uriInfo.getAbsolutePath().getPath()))
                .collect(Collectors.toList());
        return collectionDto;
    }


    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerTenants")
    public Response create(UserDto userDto) {
        UserDto returnedTenantDto = securityManager.createTenantAdmin(userDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedTenantDto.getId()).build())
                .entity(new UserDtoUriDecorator(uriInfo.getAbsolutePath().getPath()).apply(returnedTenantDto)).build();
    }



}
