package com.dematic.labs.business.picketlink;

import org.picketlink.idm.PartitionManager;
import org.picketlink.idm.model.basic.Realm;

import javax.ejb.Stateless;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("UnusedDeclaration")
@Stateless
public class SecurityManager {

    @Inject
    private PartitionManager partitionManager;

    @Produces
    @Named("supportedRealms")
    public List<String> supportedRealms() {
        return this.partitionManager
                .getPartitions(Realm.class).stream().map(Realm::getName).collect(Collectors.toList());
    }


}
