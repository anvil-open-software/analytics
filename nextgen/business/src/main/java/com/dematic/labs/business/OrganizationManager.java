package com.dematic.labs.business;

import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.persistence.entities.Organization;
import com.dematic.labs.persistence.entities.QOrganization;
import com.dematic.labs.picketlink.RealmSelector;
import com.mysema.query.jpa.impl.JPAQuery;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.annotation.Nonnull;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Stateless
public class OrganizationManager {

    private CrudService crudService;

    RealmSelector realmSelector;

    @SuppressWarnings("UnusedDeclaration")
    public OrganizationManager() {
    }

    @Inject
    public OrganizationManager(@Nonnull CrudService crudService, @Nonnull RealmSelector realmSelector) {
        this.crudService = crudService;
        this.realmSelector = realmSelector;
    }

    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public List<OrganizationDto> getOrganizations() {
       final List<OrganizationDto> organizationDTOs = new ArrayList<>();

       QOrganization qOrganization = QOrganization.organization;
       List<Organization> organizations = new JPAQuery(crudService.getEntityManager()).from(qOrganization)
               .where(qOrganization.tenantId.eq(realmSelector.select().getId())).list(qOrganization);

       for (Organization organization : organizations) {
           OrganizationDto organizationDto = new OrganizationDto();
           organizationDto.setId(organization.getId().toString());
           organizationDto.setName(organization.getName());
           organizationDTOs.add(organizationDto);
       }

        return organizationDTOs;
    }

    @RolesAllowed(ApplicationRole.CREATE_ORGANIZATIONS)
    public OrganizationDto create(@NotNull OrganizationDto organizationDto) {

        Organization organization = new Organization(UUID.fromString(realmSelector.select().getId()));
        organization.setName(organizationDto.getName());
        crudService.create(organization);

        OrganizationDto rtnValue = new OrganizationDto();
        rtnValue.setId(organization.getId().toString());
        rtnValue.setName(organization.getName());
        return rtnValue;
    }

    @RolesAllowed(ApplicationRole.VIEW_ORGANIZATIONS)
    public OrganizationDto getOrganization(UUID uuid) {
        Organization organization = crudService.findExisting(uuid, Organization.class);

        OrganizationDto rtnValue = new OrganizationDto();
        rtnValue.setId(organization.getId().toString());
        rtnValue.setName(organization.getName());
        return rtnValue;
    }
}
