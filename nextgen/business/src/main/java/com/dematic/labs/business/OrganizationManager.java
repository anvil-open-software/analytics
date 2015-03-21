package com.dematic.labs.business;

import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.entities.*;
import com.dematic.labs.persistence.query.QueryParametersHelper;
import com.mysema.query.jpa.JPQLQuery;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.annotation.Nonnull;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Stateless
public class OrganizationManager {

    public static final String ORGANIZATION_RESOURCE_PATH = "organization";
    private CrudService crudService;

    @SuppressWarnings("UnusedDeclaration")
    public OrganizationManager() {
    }

    @Inject
    public OrganizationManager(@Nonnull CrudService crudService) {
        this.crudService = crudService;
    }

    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public CollectionDto<OrganizationDto> getOrganizations(@Valid QueryParameters queryParameters) {

        QOrganization qOrganization = QOrganization.organization;

        QueryParametersHelper.convertPropertyStringsToQueryPaths(queryParameters, qOrganization);
        JPQLQuery query = crudService.createQuery(queryParameters, qOrganization);

        //QBean
        List<OrganizationDto> organizationDtoList = query.list(qOrganization)
                .stream().map(new OrganizationConverter()).collect(Collectors.toList());

        return new CollectionDto<>(organizationDtoList, queryParameters);
    }

    @RolesAllowed(ApplicationRole.CREATE_ORGANIZATIONS)
    public OrganizationDto create(@NotNull OrganizationDto organizationDto) {

        if (organizationDto.getId() != null) {
            throw new IllegalArgumentException("Organization ID must be null when creating a new organization");
        }

        return createOrUpdate(organizationDto);
    }

    @RolesAllowed(ApplicationRole.CREATE_ORGANIZATIONS)
    public OrganizationDto update(@NotNull OrganizationDto organizationDto) {

        if (organizationDto.getId() == null) {
            throw new IllegalArgumentException("Organization ID must not be null when updating an existing organization");
        }

        return createOrUpdate(organizationDto);
    }

    @Nonnull
    private OrganizationDto createOrUpdate(@NotNull OrganizationDto organizationDto) {

        Organization organization;
        if (organizationDto.getId() == null) {
            organization = crudService.createNewOwnedAsset(Organization.class);
        } else {
            organization = crudService.findExisting(Organization.class, UUID.fromString(organizationDto.getId()));
        }
        organization.setName(organizationDto.getName());

        if (organizationDto.getId() == null) {
            crudService.create(organization);
        }

        return new OrganizationConverter().apply(organization);
    }

    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public OrganizationDto getOrganization(@Nonnull UUID uuid) {
        Organization organization = crudService.findExisting(Organization.class, uuid);

        return new OrganizationConverter().apply(organization);
    }

    @RolesAllowed(ApplicationRole.ADMINISTER_ORGANIZATION_BUSINESS_ROLES)
    public OrganizationDto grantRevokeBusinessRole(OrganizationDto organizationDto) {

        if (organizationDto.getId() == null) {
            throw new IllegalArgumentException("Organization ID must not be null when granting/revoking business roles on an existing organization");
        }
        Organization organization = crudService.findExisting(Organization.class, UUID.fromString(organizationDto.getId()));

        new ArrayList<>(organization.getBusinessRoles().values()).stream()
                .map(OrganizationBusinessRole::getBusinessRole).forEach(organization::removeBusinessRole);

        new ArrayList<>(organizationDto.getBusinessRoles()).stream()
                .map(new BusinessRoleDtoConverter()).forEach(organization::addBusinessRole);

        return new OrganizationConverter().apply(organization);
    }

    private class OrganizationConverter implements Function<Organization, OrganizationDto> {

        @Override
        public OrganizationDto apply(Organization organization) {
            OrganizationDto organizationDto = new OrganizationDto();
            organizationDto.setId(organization.getId().toString());
            organizationDto.setName(organization.getName());
            organizationDto.setBusinessRoles(organization.getBusinessRoles().values().stream()
                    .map(new BusinessRoleConverter()).collect(Collectors.toList()));

            return organizationDto;
        }
    }

    private class BusinessRoleDtoConverter implements Function<OrganizationBusinessRoleDto, OrganizationBusinessRole> {
        @Override
        public OrganizationBusinessRole apply(OrganizationBusinessRoleDto organizationBusinessRoleDto) {
            return new OrganizationBusinessRole(
                    BusinessRole.valueOf(BusinessRole.class, organizationBusinessRoleDto.getBusinessRole()),
                    organizationBusinessRoleDto.isActive());
        }
    }

    private class BusinessRoleConverter implements Function<OrganizationBusinessRole, OrganizationBusinessRoleDto> {
        @Override
        public OrganizationBusinessRoleDto apply(OrganizationBusinessRole organizationBusinessRole) {
            return new OrganizationBusinessRoleDto(
                    organizationBusinessRole.getBusinessRole(),
                    organizationBusinessRole.isActive());
        }
    }
}
