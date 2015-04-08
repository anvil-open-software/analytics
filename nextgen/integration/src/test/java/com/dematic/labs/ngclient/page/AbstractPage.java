package com.dematic.labs.ngclient.page;

import org.openqa.selenium.WebDriver;

import javax.annotation.Nonnull;

public abstract class AbstractPage {

    protected final WebDriver driver;

    protected AbstractPage(@Nonnull WebDriver driver) {
        this.driver = driver;
    }
}
