package com.dematic.labs.business;

import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.persistence.entities.CrudService;
import com.dematic.labs.persistence.entities.Organization;
import com.dematic.labs.persistence.entities.QOrganization;
import com.mysema.query.jpa.JPQLQuery;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.annotation.Nonnull;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Stateless
public class OrganizationManager {

    private CrudService crudService;

    @SuppressWarnings("UnusedDeclaration")
    public OrganizationManager() {
    }

    @Inject
    public OrganizationManager(@Nonnull CrudService crudService) {
        this.crudService = crudService;
    }

    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public List<OrganizationDto> getOrganizations() {

        QOrganization qOrganization = QOrganization.organization;
        JPQLQuery query = crudService.createQuery(qOrganization).from();

        return query.list(qOrganization).stream().map(new OrganizationConverter()).collect(Collectors.toList());
    }

    @RolesAllowed(ApplicationRole.CREATE_ORGANIZATIONS)
    public OrganizationDto create(@NotNull OrganizationDto organizationDto) {

        Organization organization = crudService.createNewOwnedAsset(Organization.class);
        organization.setName(organizationDto.getName());
        crudService.create(organization);

        return new OrganizationConverter().apply(organization);
    }

    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public OrganizationDto getOrganization(UUID uuid) {
        Organization organization = crudService.findExisting(uuid, Organization.class);

        return new OrganizationConverter().apply(organization);
    }

    private class OrganizationConverter implements Function<Organization, OrganizationDto> {

        @Override
        public OrganizationDto apply(Organization organization) {
            OrganizationDto organizationDto = new OrganizationDto();
            organizationDto.setId(organization.getId().toString());
            organizationDto.setName(organization.getName());

            return organizationDto;
        }
    }

}
