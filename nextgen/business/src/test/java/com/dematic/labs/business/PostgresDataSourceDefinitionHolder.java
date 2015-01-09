package com.dematic.labs.business;

import javax.annotation.sql.DataSourceDefinition;
import javax.ejb.Stateless;

@DataSourceDefinition(name = "java:global/NextGen/NextGenDataSource",
                      className = "org.postgresql.jdbc2.optional.SimpleDataSource",
                      url = "jdbc:postgresql://127.0.0.1:5432/",
                      user = "postgres")
@Stateless
public class PostgresDataSourceDefinitionHolder {
}
