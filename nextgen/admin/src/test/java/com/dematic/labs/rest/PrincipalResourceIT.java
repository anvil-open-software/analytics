package com.dematic.labs.rest;

import com.dematic.labs.business.PrincipalManager;
import com.dematic.labs.business.dto.PrincipalDto;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.persistence.entities.Principal;
import com.dematic.labs.persistence.entities.QPrincipal;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PrincipalResourceIT {

    private WebTarget target;

    @Deployment(testable = false)
    public static WebArchive createDeployment() {

        File[] queryDslDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("com.mysema.querydsl:querydsl-jpa")
                .withTransitivity().asFile();

        return ShrinkWrap.create(WebArchive.class)
                .addClasses(
                        PrincipalResourceApplication.class, PrincipalResource.class,
                        PrincipalDto.class,
                        QPrincipal.class,
                        Principal.class,
                        PrincipalManager.class,
                        CrudService.class)
                .addAsLibraries(queryDslDependency)
                .addAsResource("META-INF/persistence.xml");
    }

    @ArquillianResource
    private URL base;

    @Before
    public void setUp() throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        target = client.target(URI.create(new URL(base, "resources/principal").toExternalForm()));
        target.register(PrincipalDto.class);
    }

    @Test
    public void test1PostViaForm() throws MalformedURLException {
        MultivaluedHashMap<String, String> map = new MultivaluedHashMap<>();
        map.add("userName", "Penny");
        Response response = target.request().post(Entity.form(map));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

        String location = response.getLocation().toString();
        String[] locationElements = location.split("/");
        String uuid = locationElements[locationElements.length-1];

        Client client = ClientBuilder.newClient();
        {
            target = client.target(URI.create(new URL(base, "resources/principal").toExternalForm()));
            target.register(PrincipalDto.class);

            PrincipalDto p = target
                    .path("{id}")
                    .resolveTemplate("id", uuid)
                    .request(MediaType.APPLICATION_XML)
                    .get(PrincipalDto.class);

            assertNotNull(p);
        }

    }

    @Test
    public void test2PostViaDto() throws MalformedURLException {
        PrincipalDto principalDto = new PrincipalDto();
        principalDto.setUsername("Fred");
        Response response = target.request().post(Entity.entity(principalDto, MediaType.APPLICATION_XML_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

        String location = response.getLocation().toString();
        String[] locationElements = location.split("/");
        String uuid = locationElements[locationElements.length-1];

        Client client = ClientBuilder.newClient();
        {
            target = client.target(URI.create(new URL(base, "resources/principal").toExternalForm()));
            target.register(PrincipalDto.class);

            PrincipalDto p = target
                    .path("{id}")
                    .resolveTemplate("id", uuid)
                    .request(MediaType.APPLICATION_XML)
                    .get(PrincipalDto.class);

            assertNotNull(p);
        }

    }

    @Test
    public void test3GetList() throws Exception {
        PrincipalDto[] list = target.request().get(PrincipalDto[].class);
        assertEquals(2, list.length);
    }
}