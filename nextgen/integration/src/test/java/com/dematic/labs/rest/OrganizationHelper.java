package com.dematic.labs.rest;

import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.business.matchers.OrganizationBusinessRoleDtoMatcher;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.IdentifiableDtoHrefMatcher;
import org.hamcrest.Matcher;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.rest.SecuredEndpointHelper.getBase;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class OrganizationHelper {
    public static OrganizationDto createOrganization(SignatureToken token, String uuid) throws MalformedURLException {

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/organization").toExternalForm()));
        target.register(OrganizationDto.class);

        OrganizationDto fromServer = signRequest(token, target
                        .path("{id}")
                        .resolveTemplate("id", uuid)
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET,
                null).get(OrganizationDto.class);

        assertThat(fromServer, new IdentifiableDtoHrefMatcher<>());
        return fromServer;
    }

    public static OrganizationDto grantBusinessRoles(SignatureToken token, OrganizationDto organizationDto
            , OrganizationBusinessRoleDto... rolesToGrantArray) throws MalformedURLException {

        List<OrganizationBusinessRoleDto> rolesToGrantList =
                Arrays.asList(rolesToGrantArray);
        organizationDto.getBusinessRoles().clear();
        organizationDto.getBusinessRoles().addAll(rolesToGrantList);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/organization").toExternalForm()));

        Response response = signRequest(token, target
                        .path("{id}/grant")
                        .resolveTemplate("id", organizationDto.getId())
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.PUT, MediaType.APPLICATION_JSON)
                .put(Entity.entity(organizationDto, MediaType.APPLICATION_JSON));

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        OrganizationDto fromServer = response.readEntity(OrganizationDto.class);

        assertThat(fromServer, new IdentifiableDtoHrefMatcher<>());
        assertThat(fromServer.getBusinessRoles(), iterableWithSize(rolesToGrantList.size()));

        List<Matcher<? super OrganizationBusinessRoleDto>> businessRoleNameMatcherList =
                rolesToGrantList.stream()
                .map(OrganizationBusinessRoleDtoMatcher::<OrganizationBusinessRoleDto>equalTo).collect(Collectors.toList());

        assertThat(fromServer.getBusinessRoles(), containsInAnyOrder(businessRoleNameMatcherList));
        return fromServer;
    }
}
