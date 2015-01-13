package com.dematic.labs.business;

import com.dematic.labs.business.dto.PrincipalDto;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.persistence.entities.Principal;
import com.dematic.labs.persistence.entities.QPrincipal;
import com.mysema.query.jpa.impl.JPAQuery;

import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Stateless
public class PrincipalManager {

    @NotNull
    private CrudService crudService;


    @SuppressWarnings("UnusedDeclaration")
    public PrincipalManager() {
    }

    @Inject
    public PrincipalManager(@NotNull CrudService crudService) {
        this.crudService = crudService;
    }

   public List<PrincipalDto> getPrincipals() {
       final List<PrincipalDto> principalDTOs = new ArrayList<>();

       QPrincipal qprincipal = QPrincipal.principal;
       List<Principal> principals = new JPAQuery(crudService.getEntityManager()).from(qprincipal).list(qprincipal);

       for (Principal principal : principals) {
           PrincipalDto principalDto = new PrincipalDto();
           principalDto.setId(principal.getId().toString());
           principalDto.setUsername(principal.getUsername());
           principalDTOs.add(principalDto);
       }

        return principalDTOs;
    }

    public PrincipalDto create(@NotNull PrincipalDto principalDto) {

        Principal principal = new Principal();
        principal.setUsername(principalDto.getUsername());
        crudService.create(principal);

        PrincipalDto rtnValue = new PrincipalDto();
        rtnValue.setId(principal.getId().toString());
        rtnValue.setUsername(principal.getUsername());
        return rtnValue;
    }

    public PrincipalDto getPrincipal(UUID uuid) {
        Principal principal = crudService.findExisting(uuid, Principal.class);

        PrincipalDto rtnValue = new PrincipalDto();
        rtnValue.setId(principal.getId().toString());
        rtnValue.setUsername(principal.getUsername());
        return rtnValue;
    }
}
