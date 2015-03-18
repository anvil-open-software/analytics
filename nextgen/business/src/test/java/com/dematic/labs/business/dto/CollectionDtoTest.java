package com.dematic.labs.business.dto;

import com.dematic.labs.business.Pagination;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CollectionDtoTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final List<TenantDto> items;

    public CollectionDtoTest() {
        items = new ArrayList<>();
        for (Integer i = 0; i<25; i++) {
            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(String.valueOf(i));
            items.add(tenantDto);
        }
    }

    @Test
    public void testPrePaginatedConstructor() {
        Pagination pagination = new Pagination(0, 10);
        CollectionDto<TenantDto> collectionDto = new CollectionDto<>(items, pagination);

        assertEquals(pagination.getOffset(), collectionDto.getOffset());
        assertEquals(pagination.getLimit(), collectionDto.getLimit());
        assertEquals(items.size(), collectionDto.getSize());
        assertEquals(items, collectionDto.getItems());
    }

    @Test
    public void testNonPaginatedConstructor() {
        Pagination pagination = new Pagination(0, 10);
        CollectionDto<TenantDto> collectionDto = new CollectionDto<>(items, pagination, true);

        assertEquals(pagination.getOffset(), collectionDto.getOffset());
        assertEquals(pagination.getLimit(), collectionDto.getLimit());
        assertEquals(pagination.getLimit(), collectionDto.getSize());
        assertEquals(items.subList(pagination.getOffset(), pagination.getLimit()), collectionDto.getItems());
    }

    @Test
    public void testNonPaginatedConstructorWithLimitOvershoot() {
        Pagination pagination = new Pagination(20, 10);
        CollectionDto<TenantDto> collectionDto = new CollectionDto<>(items, pagination, true);

        assertEquals(pagination.getOffset(), collectionDto.getOffset());
        assertEquals(pagination.getLimit(), collectionDto.getLimit());
        assertEquals(pagination.getOffset() + pagination.getLimit() - items.size(), collectionDto.getSize());
        assertEquals(items.subList(pagination.getOffset(), items.size()), collectionDto.getItems());
    }

    @Test
    public void testNonPaginatedConstructorWithOffsetOvershoot() {
        Pagination pagination = new Pagination(30, 10);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Offset [30] exceeds size of collection [25]");
        new CollectionDto<>(items, pagination, true);

    }

}