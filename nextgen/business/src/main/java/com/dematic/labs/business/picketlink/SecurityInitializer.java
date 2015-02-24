/*
 ** JBoss, Home of Professional Open Source
 ** Copyright 2013, Red Hat, Inc. and/or its affiliates, and individual
 ** contributors by the @authors tag. See the copyright.txt in the
 ** distribution for a full listing of individual contributors.
 **
 ** Licensed under the Apache License, Version 2.0 (the "License");
 ** you may not use this file except in compliance with the License.
 ** You may obtain a copy of the License at
 ** http://www.apache.org/licenses/LICENSE-2.0
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
 **/
package com.dematic.labs.business.picketlink;

import org.picketlink.event.PartitionManagerCreateEvent;
import org.picketlink.idm.IdentityManager;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.RelationshipManager;
import org.picketlink.idm.credential.Password;
import org.picketlink.idm.model.basic.Realm;
import org.picketlink.idm.model.basic.Role;
import org.picketlink.idm.model.basic.User;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.event.Observes;

import static org.picketlink.idm.model.basic.BasicModel.grantRole;

@SuppressWarnings("UnusedDeclaration")
@Singleton
@Startup
public class SecurityInitializer {

    /**
     * <p>Creates some default users for each realm/company.</p>
     */
    public void createDefaultUsers(@Observes PartitionManagerCreateEvent event) {
        PartitionManager partitionManager = event.getPartitionManager();

        partitionManager.add(new Realm(Realm.DEFAULT_REALM), "jpa.config");

        User superUser = createUserForRealm(partitionManager, "Dematic", "superuser", "abcd1234", "superuser");
        createUserForRealm(partitionManager, "Safeway", "janeAdmin", "abcd1234", "tenantAdmin");
        createUserForRealm(partitionManager, "Safeway", "joeUser", "abcd1234", "user");
    }

    private User createUserForRealm(PartitionManager partitionManager,
                                    String realmName, String loginName, String password, String roleName) {
        Realm partition = partitionManager.getPartition(Realm.class, realmName);

        if (partition == null) {
            partition = new Realm(realmName);
            partitionManager.add(partition, "jpa.config");
        }

        IdentityManager identityManager = partitionManager.createIdentityManager(partition);

        User user = new User(loginName);

        identityManager.add(user);
        identityManager.updateCredential(user, new Password(password));

        Role role = new Role(roleName);
        identityManager.add(role);

        RelationshipManager relationshipManager = partitionManager.createRelationshipManager();

        grantRole(relationshipManager, user, role);
        return user;
    }
}
