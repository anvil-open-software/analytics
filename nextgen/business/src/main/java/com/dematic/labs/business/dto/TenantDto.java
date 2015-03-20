package com.dematic.labs.business.dto;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TenantDto extends NamedDto {

    @SuppressWarnings("UnusedDeclaration") //needed for jackson json
    public TenantDto() {
    }

}
