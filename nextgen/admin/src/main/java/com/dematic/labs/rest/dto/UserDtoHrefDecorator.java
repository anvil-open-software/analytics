package com.dematic.labs.rest.dto;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;

import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityManager.ROLE_RESOURCE_PATH;
import static com.dematic.labs.business.SecurityManager.TENANT_RESOURCE_PATH;
import static com.dematic.labs.business.SecurityManager.USER_RESOURCE_PATH;

public class UserDtoHrefDecorator extends HrefDecorator<UserDto> {

    private final HrefDecorator<TenantDto> tenantDtoHrefDecorator;
    private final HrefDecorator<RoleDto> roleDtoHrefDecorator;

    public UserDtoHrefDecorator(String baseUri) {
        super(baseUri);
        this.tenantDtoHrefDecorator = new HrefDecorator<>(baseUri.replace(USER_RESOURCE_PATH, TENANT_RESOURCE_PATH));
        this.roleDtoHrefDecorator = new HrefDecorator<>(baseUri.replace(USER_RESOURCE_PATH, ROLE_RESOURCE_PATH));
    }

    @Override
    public UserDto apply(UserDto userDto) {
        super.apply(userDto);
        tenantDtoHrefDecorator.apply(userDto.getTenantDto());
        userDto.getGrantedRoles().stream().map(roleDtoHrefDecorator).collect(Collectors.toSet());
        return userDto;
    }
}
