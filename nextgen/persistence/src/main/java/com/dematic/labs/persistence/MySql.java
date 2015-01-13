package com.dematic.labs.persistence;

import java.util.HashMap;
import java.util.Map;

public class MySql extends DataSourceDefinition {


    @Override
    public Map getProperties() {
        Map<String, String> rtnValue = new HashMap<>();
        rtnValue.put("hibernate.connection.url", "jdbc:mysql://127.0.0.1:3306/nextgen");
        rtnValue.put("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");
        rtnValue.put("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
        rtnValue.put("javax.persistence.jdbc.user", "root");
        return rtnValue;
    }
}
