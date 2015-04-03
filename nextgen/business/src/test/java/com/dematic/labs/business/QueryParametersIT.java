package com.dematic.labs.business;

import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.NamedDto;
import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.business.matchers.NamedDtoMatcher;
import com.dematic.labs.business.matchers.OrganizationBusinessRoleDtoMatcher;
import com.dematic.labs.persistence.entities.*;
import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.query.SortDirection;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.dematic.labs.business.SecurityFixture.*;
import static org.junit.Assert.assertThat;


@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryParametersIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Inject
    public SecurityFixture securityFixture;

    @Inject
    OrganizationManager organizationManager;

    @Deployment
    public static WebArchive createDeployment() {

        return SecurityFixture.createDeployment("org.postgresql:postgresql",
                        PostgresDataSourceDefinitionHolder.class,
                        TwoTenantSecurityInitializer.class)
                    .addClasses(Organization.class,
                            OwnedAssetEntity.class,
                            IdentifiableEntity.class,
                            OrganizationBusinessRole.class,
                            BusinessRole.class,
                            OrganizationDto.class,
                            OrganizationBusinessRoleDto.class,
                            OrganizationBusinessRoleDtoMatcher.class,
                            NamedDtoMatcher.class,
                            QOrganization.class,
                            QOwnedAssetEntity.class,
                            QIdentifiableEntity.class,
                            QOrganizationBusinessRole.class,
                            OrganizationManager.class,
                            CrudService.class);
    }

    @Test
    public void test000PopulateOrganizations() {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        Collection<String> organizationNames = Arrays.asList("ACME", "ZEBRA", "SPAM", "NAVIS");

        organizationNames.stream().map(OrganizationDto::new).forEach(organizationManager::create);

    }

    @Test
    public void test010Pagination() {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME,
                TENANT_A_USER_PASSWORD);

        CollectionDto<OrganizationDto> collectionDto = organizationManager.getOrganizations(new QueryParameters(0, 3));
        assertThat(collectionDto.getItems(), Matchers.iterableWithSize(3));

    }

    @Test
    public void test020PaginationOffsetOvershoot() {

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME,
                TENANT_A_USER_PASSWORD);

        CollectionDto<OrganizationDto> collectionDto = organizationManager.getOrganizations(new QueryParameters(10, 5));
        //pagination beyond collection yields empty results
        assertThat(collectionDto.getItems(), Matchers.iterableWithSize(0));

    }

    @Test
    public void test030Sorting() {

        List<QueryParameters.ColumnSort> orderBy = new ArrayList<>();
        orderBy.add(new QueryParameters.ColumnSort("name", SortDirection.DESC));

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME,
                TENANT_A_USER_PASSWORD);

        CollectionDto<OrganizationDto> collectionDto = organizationManager.getOrganizations(
                new QueryParameters(0, 3, orderBy));
        assertThat(collectionDto.getItems(), Matchers.iterableWithSize(3));

        //known issue with generics and varargs
        //noinspection unchecked
        assertThat(collectionDto.getItems(), IsIterableContainingInOrder.<NamedDto>contains(
                new NamedDtoMatcher<>("ZEBRA"),
                new NamedDtoMatcher<>("SPAM"),
                new NamedDtoMatcher<>("NAVIS")));

    }

    @Test
    public void test040SortingUnknownColumn() {

        List<QueryParameters.ColumnSort> orderBy = new ArrayList<>();
        orderBy.add(new QueryParameters.ColumnSort("bad", SortDirection.DESC));

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME,
                TENANT_A_USER_PASSWORD);

        exception.expectCause(Matchers.any(IllegalArgumentException.class));
        exception.expectMessage("Unknown Property name");
        organizationManager.getOrganizations(new QueryParameters(0, 3, orderBy));

    }

    @Test
    public void test050SortingNonComparableColumn() {

        List<QueryParameters.ColumnSort> orderBy = new ArrayList<>();
        orderBy.add(new QueryParameters.ColumnSort("businessRoles", SortDirection.DESC));

        securityFixture.login(TENANT_A, TENANT_A_USER_USERNAME,
                TENANT_A_USER_PASSWORD);

        exception.expectCause(Matchers.any(IllegalArgumentException.class));
        exception.expectMessage("isn't valid for sorting (doesn't implement Comparable)");
        organizationManager.getOrganizations(new QueryParameters(0, 3, orderBy));

    }

}