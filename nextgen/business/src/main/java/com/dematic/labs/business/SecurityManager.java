package com.dematic.labs.business;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.google.common.collect.Sets;
import org.picketlink.authorization.annotations.RolesAllowed;
import org.picketlink.idm.IdentityManager;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.RelationshipManager;
import org.picketlink.idm.credential.Password;
import org.picketlink.idm.model.IdentityType;
import org.picketlink.idm.model.Partition;
import org.picketlink.idm.model.basic.*;
import org.picketlink.idm.query.IdentityQueryBuilder;
import org.picketlink.idm.query.RelationshipQuery;

import javax.annotation.Nonnull;
import javax.ejb.Stateless;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.picketlink.idm.model.basic.BasicModel.grantRole;

@Stateless
public class SecurityManager {

    public static final String TENANT_RESOURCE_PATH = "tenant";
    public static final String USER_RESOURCE_PATH = "user";
    public static final String ROLE_RESOURCE_PATH = "role";

    public static final String TENANT_CUSTOM_ROLE_PREFIX = "t_";

    @Inject
    protected PartitionManager partitionManager;

    @Inject
    protected RelationshipManager relationshipManager;

    @Inject
    protected IdentityManager identityManager;

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
        Role role = getExistingById(Role.class, id);

        if (!role.getName().startsWith(TENANT_CUSTOM_ROLE_PREFIX)) {
            throw new IllegalArgumentException("Cannot delete non Custom Tenant Role: " + role.getName());
        } else {
                identityManager.remove(role);
        }
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_USERS)
    public UserDto grantRevokeUserRole(@Nonnull UserDto userDto) {

        User user = getExistingById(User.class, userDto.getId());

        Set<Grant> existingGrants = new HashSet<>(getGrants(user));

        Set<Grant> requestedGrants = userDto.getGrantedRoles().stream()
                .map(new GrantConverter(user)).collect(Collectors.toSet());

        Sets.difference(existingGrants, requestedGrants).forEach(relationshipManager::remove);

        Sets.difference(requestedGrants, existingGrants).forEach(relationshipManager::add);

        return new UserConverter().apply(user);
    }

    private <T extends IdentityType> T getExistingById(Class<T> clazz, String id) {
        return getExistingById(identityManager, clazz, id);
    }

    private <T extends IdentityType> T getExistingById(@Nonnull IdentityManager identityManager, Class<T> clazz, String id) {
        T rtnValue = identityManager.lookupIdentityById(clazz, id);
        if (rtnValue == null) {
            throw new IllegalArgumentException("Unknown " + clazz.getSimpleName() + ": " + id);
        }
        return rtnValue;
    }

    private List<Grant> getGrants(@Nonnull User user) {

        RelationshipQuery<Grant> query = relationshipManager.createRelationshipQuery(Grant.class);

        query.setParameter(Grant.ASSIGNEE, user);
        //noinspection unchecked
        return query.getResultList();
    }

    private Set<RoleDto> getGrantedRoles(List<Grant> grants) {
        return grants.stream().map(Grant::getRole).map(new RoleConverter()).collect(Collectors.toSet());
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
            userDto.setGrantedRoles(getGrantedRoles(getGrants(user)));

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

    private class GrantConverter implements Function<RoleDto, Grant> {

        private final User user;
        public GrantConverter(User user) {
            this.user = user;
        }

        @Override
        public Grant apply(RoleDto roleDto) {
            Role role = getExistingById(Role.class, roleDto.getId());
            return new Grant(user, role);
        }
    }
}
