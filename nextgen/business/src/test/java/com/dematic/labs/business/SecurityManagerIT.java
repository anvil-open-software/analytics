package com.dematic.labs.business;

import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.picketlink.SecurityInitializer;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.picketlink.RealmSelector;
import org.apache.deltaspike.security.api.authorization.AccessDeniedException;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.picketlink.Identity;
import org.picketlink.credential.DefaultLoginCredentials;

import javax.inject.Inject;
import java.io.File;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SecurityManagerIT {

    public static final String NEW_TENANT = "NewTenant";
    public static final String TENANT_ADMIN_USERNAME = "admin@tenant.com";
    public static final String TENANT_ADMIN_PASSWORD = "adminDefault";
    public static final String TENANT_USER_USERNAME = "user@tenant.com";
    public static final String TENANT_USER_PASSWORD = "userDefault";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Inject
    SecurityManager securityManager;

    @Inject
    RealmSelector realmSelector;

    @Inject
    Identity identity;

    @Inject
    private DefaultLoginCredentials loginCredential;

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

        File[] picketLinkDependency = Maven.resolver().loadPomFromFile("pom.xml")
                .resolve("org.picketlink:picketlink")
                .withTransitivity().asFile();

        return ShrinkWrap.create(WebArchive.class)
                .addClasses(SecurityManager.class,
                        TenantDto.class,
                        UserDto.class,
                        SecurityInitializer.class,
                        RealmSelector.class,
                        CrudService.class,
                        PostgresDataSourceDefinitionHolder.class)
                .addAsLibraries(picketLinkDependency)
                .addAsLibraries(queryDslDependency)
                .addAsLibraries(postgresDependency)
                .addAsLibraries(mySqlDependency)
                .addAsResource("META-INF/beans.xml")
                .addAsResource("META-INF/persistence.xml");
    }

    @Test
    public void test01GetTenantsWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenants();
    }

    @Test
    public void test02GetTenantsWithAuthenticationAndAuthorization() throws Exception {

        login(null, null, null);
        securityManager.getTenants();
    }

    @Test
    public void test03GetTenantsWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenants();
    }

    @Test
    public void test04CreateTenantWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenant(new TenantDto());
    }

    @Test
    public void test05CreateTenantWithAuthenticationAndAuthorization() throws Exception {

        login(null, null, null);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(NEW_TENANT);
        assertNull(tenantDto.getId());

        TenantDto fromManager = securityManager.createTenant(tenantDto);
        assertNotNull(fromManager.getId());
        assertEquals(tenantDto.getName(), fromManager.getName());
    }

    @Test
    public void test06CreateDuplicateTenant() throws Exception {

        login(null, null, null);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(NEW_TENANT);
        assertNull(tenantDto.getId());

        exception.expectMessage("Duplicate Tenant Name");
        securityManager.createTenant(tenantDto);
    }

    @Test
    public void test07CreateTenantWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenant(new TenantDto());
    }

    @Test
    public void test08CreateTenantAdminWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantAdmin(new UserDto());
    }

    @Test
    public void test09CreateTenantAdminWithAuthenticationAndAuthorization() throws Exception {

        login(null, null, null);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(NEW_TENANT);
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(TENANT_ADMIN_USERNAME);
        userDto.setPassword(TENANT_ADMIN_PASSWORD);
        assertNull(userDto.getId());

        UserDto fromManager = securityManager.createTenantAdmin(userDto);
        assertNotNull(fromManager.getId());
        assertEquals(userDto.getLoginName(), fromManager.getLoginName());
    }

    @Test
    public void test10CreateDuplicateTenantAdmin() throws Exception {

        login(null, null, null);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(NEW_TENANT);
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(TENANT_ADMIN_USERNAME);
        userDto.setPassword(TENANT_ADMIN_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.User] already exists with the given identifier");
        securityManager.createTenantAdmin(userDto);
    }

    @Test
    public void test11CreateTenantAdminWithNonExistentTenant() throws Exception {

        login(null, null, null);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName("Bad Tenant");
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(TENANT_ADMIN_USERNAME);
        userDto.setPassword(TENANT_ADMIN_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("Unknown Tenant");
        securityManager.createTenantAdmin(userDto);
    }

    @Test
    public void test12CreateTenantAdminWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantAdmin(new UserDto());
    }

    @Test
    public void test13LoginAsTenantAdmin() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);
    }

    @Test
    public void test14CreateTenantUserWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantUser(new UserDto());
    }

    @Test
    public void test15CreateTenantUserWithAuthenticationAndAuthorization() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        userDto.setLoginName(TENANT_USER_USERNAME);
        userDto.setPassword(TENANT_USER_PASSWORD);
        assertNull(userDto.getId());

        UserDto fromManager = securityManager.createTenantUser(userDto);
        assertNotNull(fromManager.getId());
        assertEquals(userDto.getLoginName(), fromManager.getLoginName());
        assertEquals(NEW_TENANT, fromManager.getTenantDto().getName());
    }

    @Test
    public void test16CreateDuplicateTenantUser() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        userDto.setLoginName(TENANT_USER_USERNAME);
        userDto.setPassword(TENANT_USER_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.User] already exists with the given identifier");
        securityManager.createTenantUser(userDto);
    }

    @Test
    public void test17CreateTenantUserWithoutAuthorization() throws Exception {

        login(null, null, null);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantUser(new UserDto());
    }

    @Test
    public void test18LoginAsTenantUser() throws Exception {

        login(NEW_TENANT, TENANT_USER_USERNAME, TENANT_USER_PASSWORD);
    }


    private void login(String tenantName, String username, String password) {
        realmSelector.setRealm(tenantName != null ? tenantName : "Dematic");
        loginCredential.setUserId(username != null ? username : "superuser");
        loginCredential.setPassword(password != null ? password : "abcd1234");

        identity.login();

        assertTrue(identity.isLoggedIn());
    }

}