package com.dematic.labs.rest.dto;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;

import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityManager.ROLE_RESOURCE_PATH;
import static com.dematic.labs.business.SecurityManager.TENANT_RESOURCE_PATH;
import static com.dematic.labs.business.SecurityManager.USER_RESOURCE_PATH;

public class UserDtoUriDecorator extends UriDecorator<UserDto> {

    private final UriDecorator<TenantDto> tenantDtoUriDecorator;
    private final UriDecorator<RoleDto> roleDtoUriDecorator;

    public UserDtoUriDecorator(String baseUri) {
        super(baseUri);
        this.tenantDtoUriDecorator = new UriDecorator<>(baseUri.replace(USER_RESOURCE_PATH, TENANT_RESOURCE_PATH));
        this.roleDtoUriDecorator = new UriDecorator<>(baseUri.replace(USER_RESOURCE_PATH, ROLE_RESOURCE_PATH));
    }

    @Override
    public UserDto apply(UserDto userDto) {
        super.apply(userDto);
        tenantDtoUriDecorator.apply(userDto.getTenantDto());
        userDto.getGrantedRoles().stream().map(roleDtoUriDecorator).collect(Collectors.toSet());
        return userDto;
    }
}
