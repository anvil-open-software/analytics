package com.dematic.labs.business;

import javax.annotation.sql.DataSourceDefinition;
import javax.ejb.Stateless;

@DataSourceDefinition(name = "java:global/NextGen/NextGenDataSource",
                      className = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource",
                      url = "jdbc:mysql://127.0.0.1:3306/nextgen",
                      user = "root")
@Stateless
public class MySqlDataSourceDefinitionHolder {
}
