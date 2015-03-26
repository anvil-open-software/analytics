package com.dematic.labs.rest.dto;

import com.dematic.labs.business.dto.IdentifiableDto;

import java.util.function.Function;

public class UriDecorator<T extends IdentifiableDto> implements Function<T, T> {

    private final String baseUri;
    public UriDecorator(String baseUri) {

        this.baseUri = baseUri;
    }

    @Override
    public T apply(T identifiableDto) {
        identifiableDto.setUri(baseUri + "/" + identifiableDto.getId());

        return identifiableDto;
    }
}
