package com.dematic.labs.business;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import org.picketlink.authorization.annotations.RolesAllowed;
import org.picketlink.idm.IdentityManager;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.RelationshipManager;
import org.picketlink.idm.credential.Password;
import org.picketlink.idm.model.Partition;
import org.picketlink.idm.model.basic.*;
import org.picketlink.idm.query.IdentityQueryBuilder;
import org.picketlink.idm.query.RelationshipQuery;

import javax.ejb.Stateless;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.picketlink.idm.model.basic.BasicModel.grantRole;

@SuppressWarnings("UnusedDeclaration")
@Stateless
public class SecurityManager {

    public static final String TENANT_CUSTOM_ROLE_PREFIX = "t_";

    @Inject
    private PartitionManager partitionManager;

    @Inject
    private RelationshipManager relationshipManager;

    @Inject
    private IdentityManager identityManager;

    @Produces
    @Named("supportedRealms")
    public List<String> supportedRealms() {
        return this.partitionManager
                .getPartitions(Realm.class).stream().map(Realm::getName).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public List<TenantDto> getTenants() {
        return this.partitionManager
                .getPartitions(Realm.class).stream().map(new PartitionConverter()).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public TenantDto createTenant(@NotNull TenantDto tenantDto) {

        Realm partition = partitionManager.getPartition(Realm.class, tenantDto.getName());

        if (partition == null) {
            partition = new Realm(tenantDto.getName());
            partitionManager.add(partition);
        } else {
            throw new IllegalArgumentException("Duplicate Tenant Name");
        }
        IdentityManager identityManager = partitionManager.createIdentityManager(partition);

        for (String roleName : ApplicationRole.getTenantRoles()) {
            Role role = new Role(roleName);
            identityManager.add(role);
        }

        return new PartitionConverter().apply(partition);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public void deleteTenant(String id) {
        Realm partition = partitionManager.lookupById(Realm.class, id);

        if (partition == null) {
            throw new IllegalArgumentException("Unknown Tenant: " + id);
        } else {
            partitionManager.remove(partition);
        }
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public UserDto createTenantAdmin(@NotNull UserDto userDto) {
        Realm partition = partitionManager.getPartition(Realm.class, userDto.getTenantDto().getName());
        if (partition == null) {
            throw new IllegalArgumentException("Unknown Tenant");
        }
        IdentityManager identityManager = partitionManager.createIdentityManager(partition);

        User user = new User(userDto.getLoginName());

        identityManager.add(user);
        identityManager.updateCredential(user, new Password(userDto.getPassword()));

        for (String roleName : ApplicationRole.getTenantAdminRoles()) {

            Role role = BasicModel.getRole(identityManager, roleName);
            grantRole(relationshipManager, user, role);
        }


        return new UserConverter().apply(user);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public List<UserDto> getTenantsAdminUsers() {
        List<User> tenantsAdminUsers = new ArrayList<>();

        for (Partition partition : partitionManager.getPartitions(Realm.class)) {
            IdentityManager identityManager = partitionManager.createIdentityManager(partition);
            Role tenantAdminRole = BasicModel.getRole(identityManager, ApplicationRole.ADMINISTER_USERS);
            if (tenantAdminRole != null) {
                RelationshipQuery<Grant> query = relationshipManager.createRelationshipQuery(Grant.class);
                query.setParameter(GroupRole.ROLE, tenantAdminRole);
                for (Grant grant : query.getResultList()) {
                    try {
                        User user = (User) grant.getAssignee();
                        tenantsAdminUsers.add(user);
                    } catch (ClassCastException cce) {
                        //ignore
                    }
                }
            }

        }
        return tenantsAdminUsers.stream().map(new UserConverter()).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_USERS)
    public List<UserDto> getUsers() {
        IdentityQueryBuilder queryBuilder = identityManager.getQueryBuilder();
        //noinspection unchecked
        Collection<User> users = queryBuilder.createIdentityQuery(User.class).getResultList();
        return users.stream().map(new UserConverter()).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_USERS)
    public UserDto createTenantUser(@NotNull UserDto userDto) {

        User user = new User(userDto.getLoginName());

        identityManager.add(user);
        identityManager.updateCredential(user, new Password(userDto.getPassword()));

        return new UserConverter().apply(user);
    }

    @RolesAllowed({ApplicationRole.ADMINISTER_USERS, ApplicationRole.ADMINISTER_ROLES})
    public List<RoleDto> getRoles() {
        IdentityQueryBuilder queryBuilder = identityManager.getQueryBuilder();
        //noinspection unchecked
        List<Role> roles = queryBuilder.createIdentityQuery(Role.class).getResultList();
        return roles.stream().map(new RoleConverter()).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_ROLES)
    public RoleDto createRole(RoleDto userDto) {
        if (!userDto.getName().startsWith(TENANT_CUSTOM_ROLE_PREFIX)) {
            throw new IllegalArgumentException("Custom Tenant Role must begin with \"" + TENANT_CUSTOM_ROLE_PREFIX + "\"");
        }

        Role role = new Role(userDto.getName());
        identityManager.add(role);
        return new RoleConverter().apply(role);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_ROLES)
    public void deleteRole(String id) {
        Role role = identityManager.lookupIdentityById(Role.class, id);

        if (role == null) {
            throw new IllegalArgumentException("Unknown Role: " + id);
        } else if (!role.getName().startsWith(TENANT_CUSTOM_ROLE_PREFIX)) {
            throw new IllegalArgumentException("Cannot delete non Custom Tenant Role: " + role.getName());
        } else {
                identityManager.remove(role);
        }
    }

    private class RoleConverter implements Function<Role, RoleDto> {

        @Override
        public RoleDto apply(Role user) {
            RoleDto userDto = new RoleDto();
            userDto.setId(user.getId());
            userDto.setName(user.getName());

            return userDto;
        }
    }

    private class UserConverter implements Function<User, UserDto> {

        @Override
        public UserDto apply(User user) {
            UserDto userDto = new UserDto();
            userDto.setId(user.getId());
            userDto.setLoginName(user.getLoginName());
            userDto.setTenantDto(new PartitionConverter().apply(user.getPartition()));

            return userDto;
        }
    }

    private class PartitionConverter implements Function<Partition, TenantDto> {
        @Override
        public TenantDto apply(Partition partition) {
            TenantDto rtnValue = new TenantDto();
            rtnValue.setId(partition.getId());
            rtnValue.setName(partition.getName());

            return rtnValue;
        }
    }
}
