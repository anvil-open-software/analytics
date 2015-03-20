package com.dematic.labs.persistence.query;

import com.dematic.labs.persistence.entities.Pagination;
import com.dematic.labs.persistence.entities.SortDirection;
import com.mysema.query.types.Order;
import com.mysema.query.types.Path;
import com.mysema.query.types.path.EntityPathBase;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class PaginationHelper {
    public static void convertPropertyStringsToQueryPaths(@Nonnull Pagination pagination,
                                                          EntityPathBase<?> entityPathBase) {

        Class<?> entityPathBaseClass = entityPathBase.getClass();
        for (Pagination.ColumnSort columnSort : pagination.getOrderBy()) {

            Path<? extends Comparable> propertyField = null;
            for (Field field : entityPathBaseClass.getDeclaredFields()) {
                if (field.getName().equals(columnSort.getPropertyName())) {
                    try {
                        //noinspection unchecked
                        propertyField = (Path<? extends Comparable>) field.get(entityPathBase);
                        break;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (ClassCastException cce) {
                        throw new IllegalArgumentException("Property isn't a Path");
                    }
                }
            }
            if (propertyField == null) {
                throw new IllegalArgumentException(
                        String.format("Unknown Property name [%s]", columnSort.getPropertyName()));
            } else {
                List<Class<?>> interfaces = Arrays.asList(propertyField.getType().getInterfaces());
                if (!interfaces.contains(Comparable.class)) {
                    throw new IllegalArgumentException(
                            String.format("Property [%s] isn't valid for sorting (doesn't implement Comparable).",
                                    columnSort.getPropertyName()));
                }
            }

            columnSort.setQueryDslPath(propertyField);
            columnSort.setQueryDslOrder(columnSort.getSortDirection().equals(
                    SortDirection.ASC) ? Order.ASC : Order.DESC);

        }

    }
}
