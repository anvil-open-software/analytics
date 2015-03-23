package com.dematic.labs.business.dto;

import com.dematic.labs.persistence.entities.SortDirection;
import com.dematic.labs.persistence.query.QueryParameters;
import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.collections4.comparators.ComparatorChain;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.stream.Collectors;

@XmlRootElement
public class CollectionDto<T extends IdentifiableDto> {

    private static final String NO_SUCH_METHOD_EXCEPTION_TEXT = NoSuchMethodException.class.getSimpleName() + ":";

    private QueryParameters queryParameters;
    private List<T> items;

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public CollectionDto() {
    }

    public CollectionDto(@Nonnull List<T> items, @Nonnull QueryParameters queryParameters) {
        this(items, queryParameters, false);
    }

    public CollectionDto(@Nonnull List<T> items, @Nonnull QueryParameters queryParameters, boolean listNeedsProcessing) {
        List<T> cookedItems = items;
        int cookedLimit = queryParameters.getLimit();
        if (listNeedsProcessing) {
            List<T> sortedItems = items;
            if (!queryParameters.getOrderBy().isEmpty()) {
                ComparatorChain<T> comparatorChain = new ComparatorChain<>();

                for (QueryParameters.ColumnSort columnSort : queryParameters.getOrderBy()) {
                    BeanComparator<T> beanComparator = new BeanComparator<>(columnSort.getPropertyName());
                    comparatorChain.addComparator(beanComparator,
                            columnSort.getSortDirection().equals(SortDirection.DESC));
                }
                try {
                    sortedItems = items.stream().sorted(comparatorChain).collect(Collectors.toList());
                } catch (RuntimeException re) {
                    int startIndex = re.getMessage().lastIndexOf(NO_SUCH_METHOD_EXCEPTION_TEXT);
                    if (startIndex > -1) {
                        throw new IllegalArgumentException(
                                re.getMessage().substring(startIndex + NO_SUCH_METHOD_EXCEPTION_TEXT.length()));
                    }
                    throw re;
                }
            }

            if ((queryParameters.getOffset() + queryParameters.getLimit()) > sortedItems.size()) {
                if (queryParameters.getOffset() > sortedItems.size()) {
                    throw new IllegalStateException(
                            String.format("Offset [%d] exceeds size of collection [%d]",
                                    queryParameters.getOffset(), sortedItems.size()));
                }
                cookedLimit = sortedItems.size() - queryParameters.getOffset();
            }
            cookedItems = sortedItems.subList(queryParameters.getOffset(), queryParameters.getOffset() + cookedLimit);
        }

        this.queryParameters = queryParameters;
        this.items = cookedItems;
    }

    @Nonnull
    public List<T> getItems() {
        return items;
    }

    public int getSize() {
        return items.size();
    }

    public QueryParameters getQueryParameters() {
        return queryParameters;
    }

    @SuppressWarnings("UnusedDeclaration") //needed by jackson
    public void setItems(List<T> items) {
        this.items = items;
    }

    public void setSize(@SuppressWarnings("UnusedParameters") int size) { //needed by jackson
    }

    @SuppressWarnings("UnusedDeclaration") //needed by jackson
    public void setQueryParameters(QueryParameters queryParameters) {
        this.queryParameters = queryParameters;
    }

}
