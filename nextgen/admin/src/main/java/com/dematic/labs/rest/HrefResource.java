package com.dematic.labs.rest;

import com.dematic.labs.business.dto.IdentifiableDto;

import java.util.function.Function;

public abstract class HrefResource {

    protected class HrefDecorator<T extends IdentifiableDto> implements Function<T, T> {

        private final String baseUri;
        public HrefDecorator(String baseUri) {

            this.baseUri = baseUri;
        }

        @Override
        public T apply(T identifiableDto) {
            identifiableDto.setHref(baseUri + identifiableDto.getId());

            return identifiableDto;
        }
    }

}
