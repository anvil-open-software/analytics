package com.dematic.labs.rest;

import com.dematic.labs.business.PrincipalManager;
import com.dematic.labs.business.dto.PrincipalDto;

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
public class PrincipalResource {

    @EJB
    PrincipalManager principalManager;

    @Context
    UriInfo uriInfo;

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({"application/json"})
    public Response create(@FormParam("userName") String userName) {
        System.out.println("Creating a new item: " + userName);
        PrincipalDto principalDto = new PrincipalDto();
        principalDto.setUsername(userName);
        PrincipalDto returnedPrincipalDto = principalManager.create(principalDto);
//        return Response.ok(returnedPrincipalDto).status(Response.Status.CREATED).build();
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedPrincipalDto.getId()).build()).build();
    }

    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({"application/json", "application/xml"})
    public Response create(PrincipalDto principalDto) {
        PrincipalDto returnedPrincipalDto = principalManager.create(principalDto);
//        return Response.ok(returnedPrincipalDto).status(Response.Status.CREATED).build();
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedPrincipalDto.getId()).build()).build();
    }

    @GET
    @Produces({"application/xml"})
    @Path("{id}")
    public PrincipalDto getPerson(@PathParam("id") String id) {
        if (id == null) {
            return null;
        }
        return principalManager.getPrincipal(UUID.fromString(id));
    }

    @GET
    @Produces({"application/xml", "application/json"})
    public PrincipalDto[] getList() {
        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return principalManager.getPrincipals().toArray(new PrincipalDto[0]);
    }

}
