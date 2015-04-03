package com.dematic.labs.business;

import com.dematic.labs.business.dto.*;
import com.dematic.labs.matchers.ConstraintViolationMatcher;
import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.query.SortDirection;
import org.apache.deltaspike.security.api.authorization.AccessDeniedException;
import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.core.IsNot;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.*;

@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SecurityManagerIT {

    public static final String CUSTOM_TENANT_ROLE = "t_newRole";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Inject
    public SecurityFixture securityFixture;

    @Inject
    SecurityManager securityManager;

    @Deployment
    public static WebArchive createDeployment() {

        return SecurityFixture.createDeployment("org.postgresql:postgresql",
                PostgresDataSourceDefinitionHolder.class,
                OneTenantSecurityInitializer.class);
    }

    @Test
    public void test000GetTenantsWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenants(QueryParameters.DEFAULT);
    }

    @Test
    public void test010GetTenantsWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        CollectionDto<TenantDto> collectionDto = securityManager.getTenants(QueryParameters.DEFAULT);

        assertThat(collectionDto.getItems(), iterableWithSize(collectionDto.getSize()));
    }

    @Test
    public void test015GetTenantsWithInvalidPagination() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectCause(new ConstraintViolationMatcher("Pagination offset must be positive",
                "Pagination limit must be positive"));
        securityManager.getTenants(new QueryParameters(-1, -1));
    }

    @Test
    public void test017GetTenantsWithInvalidSort() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectCause(new ConstraintViolationMatcher("Sort Column name may not be empty"));
        List<QueryParameters.ColumnSort> orderBy = new ArrayList<>();
        orderBy.add(new QueryParameters.ColumnSort("", SortDirection.ASC));
        securityManager.getTenants(new QueryParameters(0, QueryParameters.DEFAULT_LIMIT, orderBy));
    }

    @Test
    public void test020GetTenantsWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenants(QueryParameters.DEFAULT);
    }

    @Test
    public void test030CreateTenantWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenant(new TenantDto());
    }

    @Test
    public void test040CreateTenantWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_B);
        assertNull(tenantDto.getId());

        TenantDto fromManager = securityManager.createTenant(tenantDto);
        assertNotNull(fromManager.getId());
        assertEquals(tenantDto.getName(), fromManager.getName());
    }

    @Test
    public void test050CreateDuplicateTenant() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_B);
        assertNull(tenantDto.getId());

        exception.expectMessage("Duplicate Tenant Name");
        securityManager.createTenant(tenantDto);
    }

    @Test
    public void test060CreateTenantWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

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

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_B);
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(TENANT_B_ADMIN_USERNAME);
        userDto.setPassword(TENANT_B_ADMIN_PASSWORD);
        assertNull(userDto.getId());

        UserDto fromManager = securityManager.createTenantAdmin(userDto);
        assertNotNull(fromManager.getId());
        assertEquals(userDto.getLoginName(), fromManager.getLoginName());
        assertNotNull(fromManager.getGrantedRoles());
        assertEquals(ApplicationRole.getTenantAdminRoles().size(), fromManager.getGrantedRoles().size());
    }

    @Test
    public void test090CreateDuplicateTenantAdmin() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_B);
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(TENANT_B_ADMIN_USERNAME);
        userDto.setPassword(TENANT_B_ADMIN_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.User] already exists with the given identifier");
        securityManager.createTenantAdmin(userDto);
    }

    @Test
    public void test100CreateTenantAdminWithNonExistentTenant() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName("Bad Tenant");
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(TENANT_B_ADMIN_USERNAME);
        userDto.setPassword(TENANT_B_ADMIN_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("Unknown Tenant");
        securityManager.createTenantAdmin(userDto);
    }

    @Test
    public void test110CreateTenantAdminWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantAdmin(new UserDto());
    }

    @Test
    public void test120LoginAsTenantAdmin() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
    }

    @Test
    public void test130GetTenantAdminsWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenantsAdminUsers(QueryParameters.DEFAULT);
    }

    @Test
    public void test140GetTenantAdminsWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        CollectionDto<UserDto> tenantAdminUsers = securityManager.getTenantsAdminUsers(QueryParameters.DEFAULT);

        assertNotNull(tenantAdminUsers);
        assertThat(tenantAdminUsers.getItems(), new IsNot<>(new IsEmptyIterable<>()));
    }

    @Test
    public void test150GetTenantsAdminsWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getTenantsAdminUsers(QueryParameters.DEFAULT);
    }

    @Test
    public void test160CreateTenantUserWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantUser(new UserDto());
    }

    @Test
    public void test170CreateTenantUserWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_B, TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        userDto.setLoginName(TENANT_B_USER_USERNAME);
        userDto.setPassword(TENANT_B_USER_PASSWORD);
        assertNull(userDto.getId());

        UserDto fromManager = securityManager.createTenantUser(userDto);
        assertNotNull(fromManager.getId());
        assertEquals(userDto.getLoginName(), fromManager.getLoginName());
        assertEquals(TENANT_B, fromManager.getTenantDto().getName());
    }

    @Test
    public void test180CreateDuplicateTenantUser() throws Exception {

        securityFixture.login(TENANT_B, TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD);

        UserDto userDto = new UserDto();
        userDto.setLoginName(TENANT_B_USER_USERNAME);
        userDto.setPassword(TENANT_B_USER_PASSWORD);
        assertNull(userDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.User] already exists with the given identifier");
        securityManager.createTenantUser(userDto);
    }

    @Test
    public void test190CreateTenantUserWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createTenantUser(new UserDto());
    }

    @Test
    public void test200LoginAsTenantUser() throws Exception {

        securityFixture.login(TENANT_B, TENANT_B_USER_USERNAME, TENANT_B_USER_PASSWORD);
    }


    @Test
    public void test210GetRolesWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getRoles(QueryParameters.DEFAULT);
    }

    @Test
    public void test220GetRolesWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
        CollectionDto<RoleDto> roles = securityManager.getRoles(QueryParameters.DEFAULT);

        assertNotNull(roles);
        assertThat(roles.getItems(), new IsNot<>(new IsEmptyIterable<>()));
    }

    @Test
    public void test230GetRolesWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.getRoles(QueryParameters.DEFAULT);
    }

    @Test
    public void test240CreateRoleWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.createRole(new RoleDto());
    }

    @Test
    public void test250CreateRoleWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        RoleDto roleDto = new RoleDto();
        roleDto.setName(CUSTOM_TENANT_ROLE);
        assertNull(roleDto.getId());

        RoleDto fromManager = securityManager.createRole(roleDto);
        assertNotNull(fromManager.getId());
        assertEquals(roleDto.getName(), fromManager.getName());
    }

    @Test
    public void test260CreateRoleWithBadName() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        RoleDto roleDto = new RoleDto();
        roleDto.setName("badRoleName");
        assertNull(roleDto.getId());

        exception.expectMessage("Custom Tenant Role must begin with \"t_\"");
        securityManager.createRole(roleDto);
    }

    @Test
    public void test270CreateDuplicateRole() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        RoleDto roleDto = new RoleDto();
        roleDto.setName(CUSTOM_TENANT_ROLE);
        assertNull(roleDto.getId());

        exception.expectMessage("IdentityType [class org.picketlink.idm.model.basic.Role] already exists with the given identifier");
        securityManager.createRole(roleDto);
    }

    @Test
    public void test280CreateRoleWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

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

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        CollectionDto<RoleDto> roleDtos = securityManager.getRoles(QueryParameters.DEFAULT);

        RoleDto roleToDelete = null;

        for (RoleDto roleDto : roleDtos.getItems()) {
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

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        CollectionDto<RoleDto> roleDtoCollection = securityManager.getRoles(QueryParameters.DEFAULT);

        RoleDto roleToDelete = null;

        for (RoleDto roleDto : roleDtoCollection.getItems()) {
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

        securityFixture.login(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        exception.expectMessage("Unknown Role:");
        securityManager.deleteRole("");
    }

    @Test
    public void test320DeleteRoleWithoutAuthorization() throws Exception {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.deleteRole("");
    }

    @Test
    public void test330GrantRevokeUserRoleWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.grantRevokeUserRole(new UserDto());
    }

    @Test
    public void test340GrantRevokeUserRoleWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_B, TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD);

        UserDto userDto = null;
        for (UserDto item : securityManager.getUsers(QueryParameters.DEFAULT).getItems()) {
            if (item.getGrantedRoles().size() == 0) {
                //found previously created user w/o roles
                userDto = item;
                break;
            }
        }
        assertNotNull(userDto);

        CollectionDto<RoleDto> roles = securityManager.getRoles(QueryParameters.DEFAULT);

        //simple grant
        {
            RoleDto roleDto = roles.getItems().stream()
                    .filter(p -> p.getName().equals(ApplicationRole.VIEW_ORGANIZATIONS))
                    .collect(Collectors.toList()).get(0);
            Set<RoleDto> grantedRoles = new HashSet<>();
            grantedRoles.add(roleDto);
            userDto.setGrantedRoles(grantedRoles);

            UserDto fromManager = securityManager.grantRevokeUserRole(userDto);
            assertNotNull(fromManager.getId());
            assertEquals(userDto.getId(), fromManager.getId());
            assertNotNull(fromManager.getGrantedRoles());
            assertEquals(1, fromManager.getGrantedRoles().size());
            assertEquals(ApplicationRole.VIEW_ORGANIZATIONS, fromManager.getGrantedRoles().iterator().next().getName());

        }

        //update involving both revoke and grant
        {
            RoleDto roleDto = roles.getItems().stream()
                    .filter(p -> p.getName().equals(ApplicationRole.CREATE_ORGANIZATIONS))
                    .collect(Collectors.toList()).get(0);
            Set<RoleDto> grantedRoles = new HashSet<>();
            grantedRoles.add(roleDto);
            userDto.setGrantedRoles(grantedRoles);

            UserDto fromManager = securityManager.grantRevokeUserRole(userDto);
            assertNotNull(fromManager.getId());
            assertEquals(userDto.getId(), fromManager.getId());
            assertNotNull(fromManager.getGrantedRoles());
            assertEquals(1, fromManager.getGrantedRoles().size());
            assertEquals(ApplicationRole.CREATE_ORGANIZATIONS, fromManager.getGrantedRoles().iterator().next().getName());
        }
    }

    @Test
    public void test360GrantRevokeUserRoleWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        exception.expectMessage(AccessDeniedException.class.getName());
        securityManager.grantRevokeUserRole(new UserDto());
    }

}