package com.dematic.labs.rest.matchers;

import com.dematic.labs.business.dto.IdentifiableDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;

public class CreatedResponseMatcher<T extends IdentifiableDto> extends TypeSafeDiagnosingMatcher<Response> {

    private final Response.Status status;
    private final T identifiableDto;
    private final IdentifiableDtoHrefMatcher<T> matcher;

    public CreatedResponseMatcher(@Nonnull T dto, IdentifiableDtoHrefMatcher<T> matcher) {
        this.status = Response.Status.CREATED;
        this.identifiableDto = dto;
        this.matcher = matcher;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" status [").appendText(status.toString()).appendText("] and ")
                .appendDescriptionOf(matcher);

    }

    @Override
    protected boolean matchesSafely(Response response, Description mismatchDescription) {
        String location = response.getLocation().toString();
        String[] locationElements = location.split("/");
        String uuid = locationElements[locationElements.length-1];

        if (!response.getStatusInfo().equals(status)) {
            mismatchDescription.appendText("status[ ").appendText(response.getStatusInfo().toString()).appendText("]");
            return false;
        } else if (uuid == null) {
            mismatchDescription.appendText("location uuid is null");
            return false;
        } else if (!uuid.equals(identifiableDto.getId())) {
            mismatchDescription.appendText("uuid [").appendText(uuid)
                    .appendText("] and dto id [").appendText(identifiableDto.getId()).appendText("] ");
        }

        return matcher.matchesSafely(identifiableDto, mismatchDescription);
    }

}
