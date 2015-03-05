package com.dematic.labs.business;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.picketlink.SecurityInitializer;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.picketlink.RealmSelector;
import org.apache.deltaspike.security.api.authorization.AccessDeniedException;
import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.core.IsNot;
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
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SecurityManagerIT {

    public static final String NEW_TENANT = "NewTenant";

    public static final String TENANT_ADMIN_USERNAME = "admin@tenant.com";
    public static final String TENANT_ADMIN_PASSWORD = "adminDefault";

    public static final String TENANT_USER_USERNAME = "user@tenant.com";
    public static final String TENANT_USER_PASSWORD = "userDefault";

    public static final String CUSTOM_TENANT_ROLE = "t_newRole";

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
                        RoleDto.class,
                        ApplicationRole.class,
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
    public void test000GetTenantsWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenants();
    }

    @Test
    public void test010GetTenantsWithAuthenticationAndAuthorization() throws Exception {

        login(null, null, null);
        securityManager.getTenants();
    }

    @Test
    public void test020GetTenantsWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenants();
    }

    @Test
    public void test030CreateTenantWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenant(new TenantDto());
    }

    @Test
    public void test040CreateTenantWithAuthenticationAndAuthorization() throws Exception {

        login(null, null, null);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(NEW_TENANT);
        assertNull(tenantDto.getId());

        TenantDto fromManager = securityManager.createTenant(tenantDto);
        assertNotNull(fromManager.getId());
        assertEquals(tenantDto.getName(), fromManager.getName());
    }

    @Test
    public void test050CreateDuplicateTenant() throws Exception {

        login(null, null, null);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(NEW_TENANT);
        assertNull(tenantDto.getId());

        exception.expectMessage("Duplicate Tenant Name");
        securityManager.createTenant(tenantDto);
    }

    @Test
    public void test060CreateTenantWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenant(new TenantDto());
    }

    @Test
    public void test070CreateTenantAdminWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantAdmin(new UserDto());
    }

    @Test
    public void test080CreateTenantAdminWithAuthenticationAndAuthorization() throws Exception {

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
    public void test090CreateDuplicateTenantAdmin() throws Exception {

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
    public void test100CreateTenantAdminWithNonExistentTenant() throws Exception {

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
    public void test110CreateTenantAdminWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantAdmin(new UserDto());
    }

    @Test
    public void test120LoginAsTenantAdmin() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);
    }

    @Test
    public void test130GetTenantAdminsWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenantsAdminUsers();
    }

    @Test
    public void test140GetTenantAdminsWithAuthenticationAndAuthorization() throws Exception {

        login(null, null, null);
        List<UserDto> tenantAdminUsers = securityManager.getTenantsAdminUsers();

        assertNotNull(tenantAdminUsers);
        assertThat(tenantAdminUsers, new IsNot<>(new IsEmptyIterable<>()));
    }

    @Test
    public void test150GetTenantsAdminsWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenantsAdminUsers();
    }

    @Test
    public void test160CreateTenantUserWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantUser(new UserDto());
    }

    @Test
    public void test170CreateTenantUserWithAuthenticationAndAuthorization() throws Exception {

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
    public void test180CreateDuplicateTenantUser() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        userDto.setLoginName(TENANT_USER_USERNAME);
        userDto.setPassword(TENANT_USER_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.User] already exists with the given identifier");
        securityManager.createTenantUser(userDto);
    }

    @Test
    public void test190CreateTenantUserWithoutAuthorization() throws Exception {

        login(null, null, null);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantUser(new UserDto());
    }

    @Test
    public void test200LoginAsTenantUser() throws Exception {

        login(NEW_TENANT, TENANT_USER_USERNAME, TENANT_USER_PASSWORD);
    }


    @Test
    public void test210GetRolesWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getRoles();
    }

    @Test
    public void test220GetRolesWithAuthenticationAndAuthorization() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);
        List<RoleDto> roles = securityManager.getRoles();

        assertNotNull(roles);
        assertThat(roles, new IsNot<>(new IsEmptyIterable<>()));
    }

    @Test
    public void test230GetRolesWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getRoles();
    }

    @Test
    public void test240CreateRoleWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createRole(new RoleDto());
    }

    @Test
    public void test250CreateRoleWithAuthenticationAndAuthorization() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        RoleDto roleDto = new RoleDto();
        roleDto.setName(CUSTOM_TENANT_ROLE);
        assertNull(roleDto.getId());

        RoleDto fromManager = securityManager.createRole(roleDto);
        assertNotNull(fromManager.getId());
        assertEquals(roleDto.getName(), fromManager.getName());
    }

    @Test
    public void test260CreateRoleWithBadName() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        RoleDto roleDto = new RoleDto();
        roleDto.setName("badRoleName");
        assertNull(roleDto.getId());

        exception.expectMessage("Custom Tenant Role must begin with \"t_\"");
        securityManager.createRole(roleDto);
    }

    @Test
    public void test270CreateDuplicateRole() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        RoleDto roleDto = new RoleDto();
        roleDto.setName(CUSTOM_TENANT_ROLE);
        assertNull(roleDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.Role] already exists with the given identifier");
        securityManager.createRole(roleDto);
    }

    @Test
    public void test280CreateRoleWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenant(new TenantDto());
    }

    @Test
    public void test290DeleteRoleWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.deleteRole("");
    }

    @Test
    public void test300DeleteRoleWithAuthenticationAndAuthorization() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        List<RoleDto> roleDtos = securityManager.getRoles();

        RoleDto roleToDelete = null;

        for (RoleDto roleDto : roleDtos) {
            if (roleDto.getName().startsWith(SecurityManager.TENANT_CUSTOM_ROLE_PREFIX)) {
                roleToDelete = roleDto;
                break;
            }
        }

        assertNotNull(roleToDelete);
        securityManager.deleteRole(roleToDelete.getId());
    }

    @Test
    public void test310DeleteApplicationRole() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        List<RoleDto> roleDtos = securityManager.getRoles();

        RoleDto roleToDelete = null;

        for (RoleDto roleDto : roleDtos) {
            if (!roleDto.getName().startsWith(SecurityManager.TENANT_CUSTOM_ROLE_PREFIX)) {
                roleToDelete = roleDto;
                break;
            }
        }

        assertNotNull(roleToDelete);
        exception.expectMessage("Cannot delete non Custom Tenant Role:");
        securityManager.deleteRole(roleToDelete.getId());
    }

    @Test
    public void test300DeleteNonExistentRole() throws Exception {

        login(NEW_TENANT, TENANT_ADMIN_USERNAME, TENANT_ADMIN_PASSWORD);

        exception.expectMessage("Unknown Role:");
        securityManager.deleteRole("");
    }

    @Test
    public void test320DeleteRoleWithoutAuthorization() throws Exception {

        login("Safeway", "joeUser", "abcd1234");

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.deleteRole("");
    }

    private void login(String tenantName, String username, String password) {
        realmSelector.setRealm(tenantName != null ? tenantName : "Dematic");
        loginCredential.setUserId(username != null ? username : "superuser");
        loginCredential.setPassword(password != null ? password : "abcd1234");

        identity.login();

        assertTrue(identity.isLoggedIn());
    }

}