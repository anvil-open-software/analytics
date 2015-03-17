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
package com.dematic.labs.picketlink;

import com.dematic.labs.business.ApplicationRole;
import org.picketlink.event.PartitionManagerCreateEvent;
import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.model.Partition;
import org.picketlink.idm.model.basic.Realm;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.event.Observes;

@SuppressWarnings("UnusedDeclaration")
@Singleton
@Startup
public class SecurityInitializer extends AbstractSecurityInitializer {

    public static final String INSTANCE_TENANT_NAME = "Dematic";
    public static final String INSTANCE_ADMIN_USERNAME = "superuser";
    public static final String INSTANCE_ADMIN_PASSWORD = "abcd1234";

    public void initialize(@Observes PartitionManagerCreateEvent event) {
        PartitionManager partitionManager = event.getPartitionManager();

        partitionManager.add(new Realm(Realm.DEFAULT_REALM));

        Partition instance = createPartition(partitionManager, INSTANCE_TENANT_NAME, ApplicationRole.ADMINISTER_TENANTS);

        createUserForPartition(partitionManager, instance, INSTANCE_ADMIN_USERNAME
                , INSTANCE_ADMIN_PASSWORD, ApplicationRole.ADMINISTER_TENANTS);

    }

}
