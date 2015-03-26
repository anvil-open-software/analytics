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
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityManager.USER_RESOURCE_PATH;

@RequestScoped
@Path(USER_RESOURCE_PATH)
public class UserResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerUsers")
    public CollectionDto<UserDto> getList(@DefaultValue("0") @QueryParam("offset") int offset,
                                          @DefaultValue(QueryParameters.DEFAULT_LIMIT_AS_STRING) @QueryParam("limit") int limit,
                                          @QueryParam("orderBy") String orderByClause) {

        CollectionDto<UserDto> collectionDto = securityManager.getUsers(new QueryParameters(offset, limit,
                OrderByQueryParameterConverter.convert(orderByClause)));
        collectionDto.getItems().stream()
                .map(new UserDtoUriDecorator(uriInfo.getAbsolutePath().getPath()))
                .collect(Collectors.toList());

        return collectionDto;
    }

    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerUsers")
    public Response create(UserDto userDto) {

        UserDto returnedTenantDto = securityManager.createTenantUser(userDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedTenantDto.getId()).build())
                .entity(new UserDtoUriDecorator(uriInfo.getAbsolutePath().getPath()).apply(returnedTenantDto)).build();
    }

    @PUT
    @Path("{id}/grant")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerUsers")
    public Response grant(@PathParam("id") String id, UserDto userDto) {
        String rawPath = uriInfo.getAbsolutePath().getPath();
        String cookedPath = rawPath.substring(0, rawPath.indexOf(id)-1);
        return Response.ok(new UserDtoUriDecorator(cookedPath).apply(securityManager.grantRevokeUserRole(userDto))).build();
    }

}