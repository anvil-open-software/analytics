package com.dematic.labs.business;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;


@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PostgresDataSourceIT {



    @Deployment
    public static WebArchive createDeployment() {

        File postgresDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("org.postgresql:postgresql").withoutTransitivity()
                .asSingleFile();

        return ShrinkWrap.create(WebArchive.class)
                .addClasses(PostgresDataSourceDefinitionHolder.class)
                .addAsLibraries(postgresDependency);
    }

    @Resource(lookup = "java:global/NextGen/NextGenDataSource")
    DataSource dataSource;

    @Test
    public void test0DataSource() throws Exception {
        assertThat(dataSource, is(notNullValue()));
        assertThat(dataSource.getConnection(), is(notNullValue()));
    }
}