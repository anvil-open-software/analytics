package com.dematic.labs.business;

import com.dematic.labs.business.dto.*;
import com.dematic.labs.persistence.query.QueryParameters;
import com.google.common.collect.Sets;
import org.picketlink.Identity;
import org.picketlink.authorization.annotations.LoggedIn;
import org.picketlink.authorization.annotations.RolesAllowed;
import org.picketlink.idm.IdentityManager;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.RelationshipManager;
import org.picketlink.idm.credential.Password;
import org.picketlink.idm.model.Account;
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
import javax.validation.Valid;
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

    @Inject
    private Identity identity;

    @Produces
    @Named("supportedRealms")
    public List<String> supportedRealms() {
        return this.partitionManager
                .getPartitions(Realm.class).stream().map(Realm::getName).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public CollectionDto<TenantDto> getTenants(@Valid QueryParameters queryParameters) {

        List<TenantDto> partitions = partitionManager.getPartitions(Realm.class)
                .stream().map(new PartitionConverter()).collect(Collectors.toList());
        return new CollectionDto<>(partitions, queryParameters, true);
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


        return new AgentConverter().apply(user);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_TENANTS)
    public CollectionDto<UserDto> getTenantsAdminUsers(@Valid QueryParameters queryParameters) {
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
        return new CollectionDto<>(tenantsAdminUsers.stream().map(new AgentConverter()).collect(Collectors.toList()),
                queryParameters, true);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_USERS)
    public CollectionDto<UserDto> getUsers(QueryParameters queryParameters) {
        IdentityQueryBuilder queryBuilder = identityManager.getQueryBuilder();
        //noinspection unchecked
        Collection<User> users = queryBuilder.createIdentityQuery(User.class).getResultList();
        List<UserDto> userDtoList = users.stream()
                .map(new AgentConverter())
                .collect(Collectors.toList());
        return new CollectionDto<>(userDtoList, queryParameters, true);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_USERS)
    public UserDto createTenantUser(@NotNull UserDto userDto) {

        User user = new User(userDto.getLoginName());

        identityManager.add(user);
        identityManager.updateCredential(user, new Password(userDto.getPassword()));

        return new AgentConverter().apply(user);
    }

    @RolesAllowed({ApplicationRole.ADMINISTER_USERS, ApplicationRole.ADMINISTER_ROLES})
    public CollectionDto<RoleDto> getRoles(QueryParameters queryParameters) {
        IdentityQueryBuilder queryBuilder = identityManager.getQueryBuilder();
        //noinspection unchecked
        List<Role> roles = queryBuilder.createIdentityQuery(Role.class).getResultList();
        List<RoleDto> roleDtoList = roles.stream()
                .map(new RoleConverter())
                .collect(Collectors.toList());
        return new CollectionDto<>(roleDtoList, queryParameters, true);
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

        return new AgentConverter().apply(user);
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

    private List<Grant> getGrants(@Nonnull Account account) {

        RelationshipQuery<Grant> query = relationshipManager.createRelationshipQuery(Grant.class);

        query.setParameter(Grant.ASSIGNEE, account);
        //noinspection unchecked
        return query.getResultList();
    }

    private Set<RoleDto> getGrantedRoles(List<Grant> grants) {
        return grants.stream().map(Grant::getRole).map(new RoleConverter()).collect(Collectors.toSet());
    }

    @LoggedIn
    public UserDto getAuthenticatedUser() {
        return new AgentConverter().apply((Agent) identity.getAccount());
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

    private class AgentConverter implements Function<Agent, UserDto> {

        @Override
        public UserDto apply(Agent user) {
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
