package com.dematic.labs.business;

import com.dematic.labs.business.dto.PrincipalDto;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.persistence.entities.Principal;
import com.dematic.labs.persistence.entities.QPrincipal;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.inject.Inject;
import java.io.File;
import java.util.List;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;


@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PrincipalManagerIT {

    @Inject
    PrincipalManager principalManager;

    @Deployment
    public static WebArchive createDeployment() {

        File mySqlDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("mysql:mysql-connector-java").withoutTransitivity()
                .asSingleFile();

        File postgresDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("org.postgresql:postgresql").withoutTransitivity()
                .asSingleFile();

        File[] queryDslDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("com.mysema.querydsl:querydsl-jpa")
                .withTransitivity().asFile();

        return ShrinkWrap.create(WebArchive.class)
                .addClasses(Principal.class,
                        PrincipalDto.class,
                        QPrincipal.class,
                        PrincipalManager.class,
                        CrudService.class,
                        PostgresDataSourceDefinitionHolder.class)
                .addAsLibraries(queryDslDependency)
                .addAsLibraries(postgresDependency)
                .addAsLibraries(mySqlDependency)
                .addAsResource("META-INF/persistence.xml");
    }

   @Test
    public void test1SaveAndGetPrincipal() throws Exception {

        System.out.println("Creating principal");
        PrincipalDto principalDto = new PrincipalDto();
        principalDto.setUsername("Fred");
        assertNull(principalDto.getId());

        PrincipalDto persistedPrincipal = principalManager.create(principalDto);
        assertNotNull(persistedPrincipal.getId());
        assertEquals(principalDto.getUsername(), persistedPrincipal.getUsername());
    }

    @Test
    public void test2GetPrincipals() throws Exception {

        for (String username : new String[]{"joe", "Penny"}) {
            PrincipalDto principalDto = new PrincipalDto();
            principalDto.setUsername(username);
            principalManager.create(principalDto);
        }

        List<PrincipalDto> principalDtos = principalManager.getPrincipals();
        assertEquals(3, principalDtos.size());

    }

}