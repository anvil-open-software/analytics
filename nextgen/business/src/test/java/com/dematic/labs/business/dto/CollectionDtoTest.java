package com.dematic.labs.business.dto;

import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.entities.SortDirection;
import org.joda.time.LocalDate;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CollectionDtoTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final List<SortingPaginationDto> items;

    public CollectionDtoTest() {
        long minimumMs = new LocalDate(1970, 1, 1).toDate().getTime();
        long intervalMs = new LocalDate(2000, 12, 31).toDate().getTime() - minimumMs;

        items = new ArrayList<>();
        for (Integer i = 0; i < 25; i++) {
            SortingPaginationDto sortingPaginationDto =
                    new SortingPaginationDto(this.getClass().getSimpleName() + UUID.randomUUID().toString(),
                            new LocalDate(minimumMs + (long) (Math.random() * intervalMs)));

            items.add(sortingPaginationDto);
        }

        //add several with same birthday
        LocalDate sharedBirthDay = new LocalDate(minimumMs + (long) (Math.random() * intervalMs));
        for (Integer i = 0; i < 5; i++) {
            SortingPaginationDto sortingPaginationDto =
                    new SortingPaginationDto(this.getClass().getSimpleName() + UUID.randomUUID().toString(),
                            sharedBirthDay);

            items.add(sortingPaginationDto);
        }
    }

    @Test
    public void testPreProcessedConstructor() {
        QueryParameters queryParameters = new QueryParameters(0, 10);
        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, queryParameters);

        assertEquals(queryParameters.getOffset(), collectionDto.getOffset());
        assertEquals(queryParameters.getLimit(), collectionDto.getLimit());
        assertEquals(items.size(), collectionDto.getSize());
        assertEquals(items, collectionDto.getItems());
    }

    @Test
    public void testNonPreprocessedConstructor() {
        QueryParameters queryParameters = new QueryParameters(0, 10);
        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, queryParameters, true);

        assertEquals(queryParameters.getOffset(), collectionDto.getOffset());
        assertEquals(queryParameters.getLimit(), collectionDto.getLimit());
        assertEquals(queryParameters.getLimit(), collectionDto.getSize());
        assertEquals(items.subList(queryParameters.getOffset(), queryParameters.getLimit()), collectionDto.getItems());
    }

    @Test
    public void testWithLimitOvershoot() {
        QueryParameters queryParameters = new QueryParameters(25, 10);
        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, queryParameters, true);

        assertEquals(queryParameters.getOffset(), collectionDto.getOffset());
        assertEquals(queryParameters.getLimit(), collectionDto.getLimit());
        assertEquals(queryParameters.getOffset() + queryParameters.getLimit() - items.size(), collectionDto.getSize());
        assertEquals(items.subList(queryParameters.getOffset(), items.size()), collectionDto.getItems());
    }

    @Test
    public void testWithOffsetOvershoot() {
        QueryParameters queryParameters = new QueryParameters(35, 10);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(String.format("Offset [%d] exceeds size of collection [%d]",
                queryParameters.getOffset(), items.size()));
        new CollectionDto<>(items, queryParameters, true);

    }

    @Test
    public void testWithSort() {
        List<QueryParameters.ColumnSort> orderBy = new ArrayList<>();

        orderBy.add(new QueryParameters.ColumnSort("birthday", SortDirection.ASC));
        orderBy.add(new QueryParameters.ColumnSort("name", SortDirection.DESC));
        QueryParameters queryParameters = new QueryParameters(0, items.size(), orderBy);

        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, queryParameters, true);

        List<SortingPaginationDto> sortedItems = items.stream().sorted(new Comparator<SortingPaginationDto>() {
            @Override
            public int compare(SortingPaginationDto o1, SortingPaginationDto o2) {
                if (o1.getBirthday().compareTo(o2.getBirthday()) != 0) {
                    return o1.getBirthday().compareTo(o2.getBirthday());
                }
                if (o2.getName().compareTo(o1.getName()) != 0) {
                    return o2.getName().compareTo(o1.getName());
                }
                return 0;
            }
        }).collect(Collectors.toList());

        assertEquals(sortedItems, collectionDto.getItems());

    }

    @Test
    public void testWithSortUnknownColumn() {

        List<QueryParameters.ColumnSort> orderBy = new ArrayList<>();
        orderBy.add(new QueryParameters.ColumnSort("unknown", SortDirection.ASC));

        QueryParameters queryParameters = new QueryParameters(0, items.size(), orderBy);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown property 'unknown' on class 'class com.dematic.labs.business.dto.SortingPaginationDto'");

        new CollectionDto<>(items, queryParameters, true);

    }
}