package com.dematic.labs.persistence;

import java.util.HashMap;
import java.util.Map;

public class Derby extends DataSourceDefinition {


    @Override
    public Map getProperties() {
        Map<String, String> rtnValue = new HashMap<>();
        rtnValue.put("hibernate.connection.url", "jdbc:derby:test;create=true");
        rtnValue.put("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver");
        rtnValue.put("hibernate.dialect", "org.hibernate.dialect.DerbyDialect");
        rtnValue.put("hibernate.connection.password", "sa");
        return rtnValue;
    }
}
