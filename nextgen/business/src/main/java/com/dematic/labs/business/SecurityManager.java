package com.dematic.labs.business;

import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import org.picketlink.authorization.annotations.RolesAllowed;
import org.picketlink.idm.IdentityManager;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.RelationshipManager;
import org.picketlink.idm.credential.Password;
import org.picketlink.idm.model.Partition;
import org.picketlink.idm.model.basic.BasicModel;
import org.picketlink.idm.model.basic.Realm;
import org.picketlink.idm.model.basic.Role;
import org.picketlink.idm.model.basic.User;

import javax.ejb.Stateless;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.picketlink.idm.model.basic.BasicModel.grantRole;

@SuppressWarnings("UnusedDeclaration")
@Stateless
public class SecurityManager {

    @Inject
    private PartitionManager partitionManager;

    @Inject
    private IdentityManager identityManager;

    @Produces
    @Named("supportedRealms")
    public List<String> supportedRealms() {
        return this.partitionManager
                .getPartitions(Realm.class).stream().map(Realm::getName).collect(Collectors.toList());
    }

    @RolesAllowed("administerTenants")
    public List<TenantDto> getTenants() {
        return this.partitionManager
                .getPartitions(Realm.class).stream().map(new PartitionConverter()).collect(Collectors.toList());
    }

    @RolesAllowed("administerTenants")
    public TenantDto createTenant(@NotNull TenantDto tenantDto) {

        Realm partition = partitionManager.getPartition(Realm.class, tenantDto.getName());

        if (partition == null) {
            partition = new Realm(tenantDto.getName());
            partitionManager.add(partition);
        } else {
            throw new IllegalArgumentException("Duplicate Tenant Name");
        }
        IdentityManager identityManager = partitionManager.createIdentityManager(partition);
        Role role = new Role("administerUsers");
        identityManager.add(role);

        return new PartitionConverter().apply(partition);
    }

    @RolesAllowed("administerTenants")
    public UserDto createTenantAdmin(@NotNull UserDto userDto) {
        Realm partition = partitionManager.getPartition(Realm.class, userDto.getTenantDto().getName());
        if (partition == null) {
            throw new IllegalArgumentException("Unknown Tenant");
        }
        IdentityManager identityManager = partitionManager.createIdentityManager(partition);

        User user = new User(userDto.getLoginName());

        identityManager.add(user);
        identityManager.updateCredential(user, new Password(userDto.getPassword()));

        Role role = BasicModel.getRole(identityManager, "administerUsers");

        RelationshipManager relationshipManager = partitionManager.createRelationshipManager();

        grantRole(relationshipManager, user, role);

        return new UserConverter().apply(user);
    }

    @RolesAllowed("administerUsers")
    public UserDto createTenantUser(@NotNull UserDto userDto) {

        User user = new User(userDto.getLoginName());

        identityManager.add(user);
        identityManager.updateCredential(user, new Password(userDto.getPassword()));

        return new UserConverter().apply(user);
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
