package com.dematic.labs.business;

import com.dematic.labs.business.dto.*;
import com.dematic.labs.matchers.ConstraintViolationMatcher;
import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.query.SortDirection;
import com.dematic.labs.persistence.query.QueryParametersHelper;
import com.dematic.labs.picketlink.AbstractSecurityInitializer;
import com.dematic.labs.picketlink.RealmSelector;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.picketlink.Identity;
import org.picketlink.credential.DefaultLoginCredentials;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.File;

public class SecurityFixture {

    public static final String TENANT_A = "TenantA";

    public static final String TENANT_A_ADMIN_USERNAME = "admin@tenantA.com";
    public static final String TENANT_A_ADMIN_PASSWORD = "adminDefaultA";

    public static final String TENANT_A_USER_USERNAME = "user@tenantA.com";
    public static final String TENANT_A_USER_PASSWORD = "userDefaultA";

    public static final String TENANT_B = "TenantB";

    public static final String TENANT_B_ADMIN_USERNAME = "admin@tenantB.com";
    public static final String TENANT_B_ADMIN_PASSWORD = "adminDefaultB";

    public static final String TENANT_B_USER_USERNAME = "user@tenantB.com";
    public static final String TENANT_B_USER_PASSWORD = "userDefaultB";

    @Inject
    RealmSelector realmSelector;

    @Inject
    Identity identity;

    @Inject
    private DefaultLoginCredentials loginCredential;

    public SecurityFixture() {
    }

    public static WebArchive createDeployment(@Nonnull String databaseDependencyCoordinates,
                                              @Nonnull Class<?> dataSourceDefinitionHolderClass,
                                              @Nonnull Class<?> securityInitializer) {
        File databaseDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve(databaseDependencyCoordinates).withoutTransitivity()
                .asSingleFile();

        File[] queryDslDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("com.mysema.querydsl:querydsl-jpa")
                .withTransitivity().asFile();

        File[] commonsCollectionDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("org.apache.commons:commons-collections4:4.0")
                .withTransitivity().asFile();

        File[] commonsBeanUtilsDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("commons-beanutils:commons-beanutils:1.9.2")
                .withTransitivity().asFile();

        File[] picketLinkDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("org.picketlink:picketlink")
                .withTransitivity().asFile();

        return ShrinkWrap.create(WebArchive.class)
                .addClasses(SecurityManager.class,
                        TenantDto.class,
                        UserDto.class,
                        RoleDto.class,
                        NamedDto.class,
                        IdentifiableDto.class,
                        QueryParameters.class,
                        QueryParametersHelper.class,
                        SortDirection.class,
                        CollectionDto.class,
                        ConstraintViolationMatcher.class,
                        ApplicationRole.class,
                        securityInitializer,
                        RealmSelector.class,
                        dataSourceDefinitionHolderClass,
                        SecurityFixture.class,
                        AbstractSecurityInitializer.class)
                .addAsLibraries(picketLinkDependency)
                .addAsLibraries(queryDslDependency)
                .addAsLibraries(commonsCollectionDependency)
                .addAsLibraries(commonsBeanUtilsDependency)
                .addAsLibraries(databaseDependency)
                .addAsResource("META-INF/beans.xml")
                .addAsResource("META-INF/persistence.xml");
    }

    public boolean login(String tenantName, String username, String password) {
        realmSelector.setRealm(tenantName);
        loginCredential.setUserId(username);
        loginCredential.setPassword(password);

        identity.login();

        return identity.isLoggedIn();
    }

}
