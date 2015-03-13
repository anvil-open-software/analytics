package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.persistence.entities.BusinessRole;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.enterprise.context.RequestScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RequestScoped
@Path("businessRole")
public class BusinessRoleResource {

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public List<String> getList() {
        return Arrays.asList(BusinessRole.values()).stream()
                .map(BusinessRole::name).collect(Collectors.toList());
    }

}
