package com.dematic.labs.ngclient.page;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

public class HomePage extends AbstractPage {

    @FindBy(id = "welcome")
    private WebElement welcome;

    public HomePage(WebDriver driver) {
        super(driver);
    }

    public String getWelcome() {
        return welcome.getText();
    }
}
