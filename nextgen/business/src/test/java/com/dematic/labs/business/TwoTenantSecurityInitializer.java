package com.dematic.labs.business;

import com.dematic.labs.picketlink.AbstractSecurityInitializer;
import org.picketlink.event.PartitionManagerCreateEvent;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.model.Partition;
import org.picketlink.idm.model.basic.Realm;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.event.Observes;

import static com.dematic.labs.business.SecurityFixture.*;

@SuppressWarnings("UnusedDeclaration")
@Singleton
@Startup
class TwoTenantSecurityInitializer extends AbstractSecurityInitializer {

    public void initialize(@Observes PartitionManagerCreateEvent event) {
        PartitionManager partitionManager = event.getPartitionManager();

        partitionManager.add(new Realm(Realm.DEFAULT_REALM));

        Partition instance = createPartition(partitionManager, INSTANCE_TENANT_NAME, ApplicationRole.ADMINISTER_TENANTS);

        createUserForPartition(partitionManager, instance
                , INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD, ApplicationRole.ADMINISTER_TENANTS);


        //noinspection ToArrayCallWithZeroLengthArrayArgument
        Partition tenantA = createPartition(partitionManager
                , TENANT_A, ApplicationRole.getTenantRoles().toArray(new String[]{}));

        //noinspection ToArrayCallWithZeroLengthArrayArgument
        createUserForPartition(partitionManager, tenantA, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD
                , ApplicationRole.getTenantAdminRoles().toArray(new String[]{}));

        createUserForPartition(partitionManager, tenantA, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD
                , ApplicationRole.CREATE_ORGANIZATIONS,
                ApplicationRole.VIEW_ORGANIZATIONS, ApplicationRole.ADMINISTER_ORGANIZATION_BUSINESS_ROLES);


        //noinspection ToArrayCallWithZeroLengthArrayArgument
        Partition tenantB = createPartition(partitionManager
                , TENANT_B, ApplicationRole.getTenantRoles().toArray(new String[]{}));

        //noinspection ToArrayCallWithZeroLengthArrayArgument
        createUserForPartition(partitionManager, tenantB, TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD
                , ApplicationRole.getTenantAdminRoles().toArray(new String[]{}));

        createUserForPartition(partitionManager, tenantB, TENANT_B_USER_USERNAME, TENANT_B_USER_PASSWORD
                , ApplicationRole.CREATE_ORGANIZATIONS);

    }

}
