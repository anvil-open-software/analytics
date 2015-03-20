package com.dematic.labs.rest;

import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.business.matchers.OrganizationBusinessRoleDtoMatcher;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.IdentifiableDtoHrefMatcher;
import org.hamcrest.Matcher;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.rest.SecuredEndpointHelper.BASE_URL;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class OrganizationHelper {
    public static OrganizationDto getOrganization(SignatureToken token, String uuid) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/organization/{id}")
                .resolveTemplate("id", uuid)
                .request(MediaType.APPLICATION_JSON);

        OrganizationDto fromServer = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(OrganizationDto.class);

        assertThat(fromServer, new IdentifiableDtoHrefMatcher<>());
        return fromServer;
    }

    public static OrganizationDto grantBusinessRoles(SignatureToken token, OrganizationDto organizationDto
            , OrganizationBusinessRoleDto... rolesToGrantArray) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/organization/{id}/grant")
                .resolveTemplate("id", organizationDto.getId())
                .request(MediaType.APPLICATION_JSON);

        List<OrganizationBusinessRoleDto> rolesToGrantList =
                Arrays.asList(rolesToGrantArray);
        organizationDto.getBusinessRoles().clear();
        organizationDto.getBusinessRoles().addAll(rolesToGrantList);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.PUT, MediaType.APPLICATION_JSON))
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
