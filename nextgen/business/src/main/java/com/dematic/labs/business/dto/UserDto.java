package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UserDto {

    private String id;
    private TenantDto tenantDto;
    private String loginName;
    private String password;

    public UserDto() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TenantDto getTenantDto() {
        return tenantDto;
    }

    public void setTenantDto(TenantDto tenantDto) {
        this.tenantDto = tenantDto;
    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(@NotNull String loginName) {
        this.loginName = loginName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(@NotNull String password) {
        this.password = password;
    }
}
