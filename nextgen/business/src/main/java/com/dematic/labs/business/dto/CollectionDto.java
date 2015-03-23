package com.dematic.labs.business.dto;

import com.dematic.labs.business.Pagination;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class CollectionDto<T extends IdentifiableDto> {

    private int offset;
    private int limit;
    private int size;

    private List<T> items;

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public CollectionDto() {
    }

    public CollectionDto(@Nonnull List<T> items, @Nonnull Pagination pagination) {
        this(items, pagination, false);
    }

    public CollectionDto(@Nonnull List<T> items, @Nonnull Pagination pagination, boolean listNeedsPagination) {
        List<T> cookedItems = items;
        int cookedLimit = pagination.getLimit();
        if (listNeedsPagination) {
            if ((pagination.getOffset() + pagination.getLimit()) > items.size()) {
                if (pagination.getOffset() > items.size()) {
                    throw new IllegalStateException(
                            String.format("Offset [%d] exceeds size of collection [%d]",
                                    pagination.getOffset(), items.size()));
                }
                cookedLimit = items.size() - pagination.getOffset();
            }
            cookedItems = items.subList(pagination.getOffset(), pagination.getOffset() + cookedLimit);
        }

        this.offset = pagination.getOffset();
        this.limit = pagination.getLimit();
        this.size = cookedItems.size();
        this.items = cookedItems;
    }

    @Nonnull
    public List<T> getItems() {
        return items;
    }

    public int getSize() {
        return size;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setOffset(int offset) {
        this.offset = offset;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setSize(int size) {
        this.size = size;
    }

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public void setItems(List<T> items) {
        this.items = items;
    }

}
