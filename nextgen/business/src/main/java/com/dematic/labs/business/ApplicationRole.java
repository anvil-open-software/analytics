package com.dematic.labs.business;

import java.util.ArrayList;
import java.util.List;

public class ApplicationRole {

    public static final String ADMINISTER_TENANTS = "administerTenants";

    public static final String ADMINISTER_ROLES = "administerRoles";
    public static final String ADMINISTER_USERS = "administerUsers";
    public static final String VIEW_ORGANIZATIONS = "viewOrganizations";
    public static final String CREATE_ORGANIZATIONS = "createOrganizations";


    public static List<String> getTenantRoles() {
        List<String> rtnValue = new ArrayList<>();
        rtnValue.add(ADMINISTER_ROLES);
        rtnValue.add(ADMINISTER_USERS);
        rtnValue.add(VIEW_ORGANIZATIONS);
        rtnValue.add(CREATE_ORGANIZATIONS);
        return  rtnValue;
    }

    public static List<String> getTenantAdminRoles() {
        List<String> rtnValue = new ArrayList<>();
        rtnValue.add(ADMINISTER_ROLES);
        rtnValue.add(ADMINISTER_USERS);
        return  rtnValue;
    }


}
