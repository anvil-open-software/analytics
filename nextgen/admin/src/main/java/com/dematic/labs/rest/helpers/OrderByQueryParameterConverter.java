package com.dematic.labs.rest.helpers;

import com.dematic.labs.business.dto.Pagination.ColumnSort;
import com.dematic.labs.business.dto.SortDirection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class OrderByQueryParameterConverter {

    @Nonnull
    public static List<ColumnSort> convert(@Nullable String orderByClause) {
        List<ColumnSort> columnSortList = new ArrayList<>();
        if (orderByClause == null) {
            return columnSortList;
        }

        for (String orderStatement : orderByClause.split(",")) {
            orderStatement = orderStatement.trim();
            final ColumnSort columnSort;
            String[] orderStatementElements = orderStatement.split("[ ]+");
            if (orderStatementElements.length > 0) {
                if (orderStatementElements[0].isEmpty()) {
                    throw new IllegalArgumentException("Column name must not be empty");
                }
            }
            if (orderStatementElements.length == 1) {
                columnSort = new ColumnSort(orderStatementElements[0], SortDirection.ASC);
            } else if (orderStatementElements.length == 2) {
                SortDirection direction = SortDirection.valueOf(orderStatementElements[1].toUpperCase());
                if (direction == null) {
                    throw new IllegalArgumentException(String.format(
                            "Unknown Sort Direction [%s] associated with column name [%s]",
                            orderStatementElements[1], orderStatementElements[0]));
                }
                columnSort = new ColumnSort(orderStatementElements[0], direction);
            } else {
                throw new IllegalArgumentException("Unknown format for order statement: " + orderStatement);
            }

            columnSortList.add(columnSort);
        }
        return columnSortList;
    }
}
