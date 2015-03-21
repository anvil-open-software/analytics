package com.dematic.labs.rest.helpers;

import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.entities.SortDirection;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class OrderByQueryParameterConverterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNullClause() {
        List<QueryParameters.ColumnSort> orderBy = OrderByQueryParameterConverter.convert(null);
        assertThat(orderBy, empty());
    }

    @Test
    public void testNoColumnName() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column name must not be empty");
        OrderByQueryParameterConverter.convert("");
    }

    @Test
    public void testOrderStatementWithJustColumn() {
        List<QueryParameters.ColumnSort> orderBy = OrderByQueryParameterConverter.convert("column1");
        assertThat(orderBy, iterableWithSize(1));
        assertThat(orderBy, contains(new ColumnSortMatcher("column1", SortDirection.ASC)));
    }

    @Test
    public void testOrderStatementWithColumnAndSortDirection() {
        List<QueryParameters.ColumnSort> orderBy = OrderByQueryParameterConverter.convert("column1 dEsC");
        assertThat(orderBy, iterableWithSize(1));
        assertThat(orderBy, contains(new ColumnSortMatcher("column1", SortDirection.DESC)));
    }

    @Test
    public void testOrderStatementWithBadOrderStatement() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown format for order statement:");
        OrderByQueryParameterConverter.convert("column1 dEsC extra");
    }

    @Test
    public void testOrderStatementWithMultipleOrderStatements() {
        List<QueryParameters.ColumnSort> orderBy = OrderByQueryParameterConverter.convert("column1 dEsC,column2");
        assertThat(orderBy, iterableWithSize(2));
        assertThat(orderBy, IsIterableContainingInOrder.contains(new ColumnSortMatcher("column1", SortDirection.DESC),
                new ColumnSortMatcher("column2", SortDirection.ASC)));
    }


    @Test
    public void testOrderStatementWithExcessWhitespace() {
        List<QueryParameters.ColumnSort> orderBy = OrderByQueryParameterConverter.convert(" column1  dEsC, column2");
        assertThat(orderBy, iterableWithSize(2));
        assertThat(orderBy, IsIterableContainingInOrder.contains(new ColumnSortMatcher("column1", SortDirection.DESC),
                new ColumnSortMatcher("column2", SortDirection.ASC)));
    }


}