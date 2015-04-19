package com.dematic.labs.ngclient.page;

import com.dematic.labs.rest.SecuredEndpointHelper;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public class LoginPage extends AbstractPage {

    public static final EnumSet<CssMatcher> THIN_GREY = EnumSet.of(CssMatcher.THIN, CssMatcher.GREY);
    public static final EnumSet<CssMatcher> THIN_BLUE = EnumSet.of(CssMatcher.THIN, CssMatcher.BLUE);
    public static final EnumSet<CssMatcher> THICK_BLUE = EnumSet.of(CssMatcher.THICK, CssMatcher.BLUE);
    public static final EnumSet<CssMatcher> THICK_GOLD = EnumSet.of(CssMatcher.THICK, CssMatcher.GOLD);
    public static final EnumSet<CssMatcher> THICK_RED = EnumSet.of(CssMatcher.THICK, CssMatcher.RED);

    public static final EnumSet<CssMatcher> DARK_BLUE_BACKGROUND = EnumSet.of(CssMatcher.DARK_BLUE_BACKGROUND);
    public static final EnumSet<CssMatcher> DARK_GREY_BACKGROUND = EnumSet.of(CssMatcher.DARK_GREY_BACKGROUND);
    public static final EnumSet<CssMatcher> LIGHT_GREY_BACKGROUND = EnumSet.of(CssMatcher.LIGHT_GREY_BACKGROUND);

     private static final String BASE_URL_TENANT = SecuredEndpointHelper.SCHEME + "://" + SecuredEndpointHelper.HOSTNAME + "/" + SecuredEndpointHelper.CONTEXT_ROOT + "?tenant=";

    @FindBy(name="tenant")
    private WebElement tenant;

    @FindBy(name="username")
    private WebElement username;

    @FindBy(name="password")
    private WebElement password;

    @FindBy(name="signin")
    private WebElement signin;

    @FindBy(name="server-errors")
    private WebElement serverError;

    @FindBy(name="client-errors")
    private WebElement clientErrors;

    @FindBy(name="signin")
    private WebElement signInButton;

    public LoginPage(WebDriver driver) {
        super(driver);
    }

    public static LoginPage navigateTo(WebDriver driver) {
        driver.get(SecuredEndpointHelper.BASE_URL);
        return PageFactory.initElements(driver, LoginPage.class);
    }

    public static LoginPage navigateTo(WebDriver driver, String tenant) {
        String url = BASE_URL_TENANT + tenant;
        driver.get(url);
        return PageFactory.initElements(driver, LoginPage.class);
    }

    public String getTitle() {
        return driver.getTitle();
    }

    public HomePage login(String username, String password) {

        performEntryAndSubmit(username, password);

        return PageFactory.initElements(driver, HomePage.class);
    }

    public HomePage login(String tenant, String username, String password) {

        performEntryAndSubmit(tenant, username, password);

        return PageFactory.initElements(driver, HomePage.class);
    }

    public LoginPage loginExpectingFailure(String username, String password) {

        performEntryAndSubmit(username, password);

        new WebDriverWait(driver, 2).until(ExpectedConditions
                .visibilityOfElementLocated(By.xpath("//div[@name='server-errors']")));

        return PageFactory.initElements(driver, LoginPage.class);
    }

    public LoginPage loginExpectingFailure(String tenant, String username, String password) {

        performEntryAndSubmit(tenant, username, password);

        new WebDriverWait(driver, 2).until(ExpectedConditions
                .visibilityOfElementLocated(By.xpath("//div[@name='server-errors']")));

        return PageFactory.initElements(driver, LoginPage.class);
    }

    private void performEntryAndSubmit(String username, String password) {
        this.username.clear();
        this.username.sendKeys(username);

        this.password.clear();
        this.password.sendKeys(password);

        signin.click();
    }

    private void performEntryAndSubmit(String tenant, String username, String password) {
        this.tenant.clear();
        this.tenant.sendKeys(tenant);

        this.username.clear();
        this.username.sendKeys(username);

        this.password.clear();
        this.password.sendKeys(password);

        signin.click();
    }

    public WebElement getServerErrorDiv() {
        return this.serverError;
    }

    public String getServerError() {
        if (!serverError.isDisplayed()) {
            return "Not Visible";
        }

        return serverError.getText();
    }

    public WebElement getTenant() { return tenant; }

    public WebElement getUsername() {
        return username;
    }

    public WebElement getPassword() {
        return password;
    }

    public void clickTenant() { this.tenant.click(); }

    public void clickUsername() {

        this.username.click();
    }

    public void clickPassword() {
        this.password.click();
    }

    public void typeUsername(String username) {
        this.username.sendKeys(username);
    }

    public void typePassword(String password) {
        this.password.sendKeys(password);
    }

    public void clickSignIn() {
        this.signin.click();
    }

    public WebElement getSignInButton() {
        return signInButton;
    }

    public List<String> getClientErrors() {
        List<String> rtnValue = new ArrayList<>();
        if (!clientErrors.isDisplayed()) {
            rtnValue.add("Not Visible");
        } else {
            rtnValue = Arrays.asList(clientErrors.getText().split("\n"))
                    .stream().filter(p-> !p.isEmpty()).collect(Collectors.toList());
        }

        return rtnValue;
    }
}
