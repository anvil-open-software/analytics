package com.dematic.labs.business.dto;

import com.dematic.labs.persistence.entities.Pagination;
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
        Pagination pagination = new Pagination(0, 10);
        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, pagination);

        assertEquals(pagination.getOffset(), collectionDto.getOffset());
        assertEquals(pagination.getLimit(), collectionDto.getLimit());
        assertEquals(items.size(), collectionDto.getSize());
        assertEquals(items, collectionDto.getItems());
    }

    @Test
    public void testNonPreprocessedConstructor() {
        Pagination pagination = new Pagination(0, 10);
        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, pagination, true);

        assertEquals(pagination.getOffset(), collectionDto.getOffset());
        assertEquals(pagination.getLimit(), collectionDto.getLimit());
        assertEquals(pagination.getLimit(), collectionDto.getSize());
        assertEquals(items.subList(pagination.getOffset(), pagination.getLimit()), collectionDto.getItems());
    }

    @Test
    public void testWithLimitOvershoot() {
        Pagination pagination = new Pagination(25, 10);
        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, pagination, true);

        assertEquals(pagination.getOffset(), collectionDto.getOffset());
        assertEquals(pagination.getLimit(), collectionDto.getLimit());
        assertEquals(pagination.getOffset() + pagination.getLimit() - items.size(), collectionDto.getSize());
        assertEquals(items.subList(pagination.getOffset(), items.size()), collectionDto.getItems());
    }

    @Test
    public void testWithOffsetOvershoot() {
        Pagination pagination = new Pagination(35, 10);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(String.format("Offset [%d] exceeds size of collection [%d]",
                pagination.getOffset(), items.size()));
        new CollectionDto<>(items, pagination, true);

    }

    @Test
    public void testWithSort() {
        List<Pagination.ColumnSort> orderBy = new ArrayList<>();

        orderBy.add(new Pagination.ColumnSort("birthday", SortDirection.ASC));
        orderBy.add(new Pagination.ColumnSort("name", SortDirection.DESC));
        Pagination pagination = new Pagination(0, items.size(), orderBy);

        CollectionDto<SortingPaginationDto> collectionDto = new CollectionDto<>(items, pagination, true);

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

        List<Pagination.ColumnSort> orderBy = new ArrayList<>();
        orderBy.add(new Pagination.ColumnSort("unknown", SortDirection.ASC));

        Pagination pagination = new Pagination(0, items.size(), orderBy);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown property 'unknown' on class 'class com.dematic.labs.business.dto.SortingPaginationDto'");

        new CollectionDto<>(items, pagination, true);

    }
}