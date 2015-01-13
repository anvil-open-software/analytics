package com.dematic.labs.persistence;

import java.util.HashMap;
import java.util.Map;

public class EmbeddedH2 extends DataSourceDefinition {


    @Override
    public Map getProperties() {
        Map<String, String> rtnValue = new HashMap<>();
        rtnValue.put("hibernate.connection.url", "jdbc:h2:mem:test");
        rtnValue.put("hibernate.connection.driver_class", "org.h2.Driver");
        rtnValue.put("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
        rtnValue.put("hibernate.connection.password", "sa");
        return rtnValue;
    }
}
