package com.dematic.labs.ngclient;

import com.dematic.labs.ngclient.page.*;
import com.dematic.labs.picketlink.SecurityInitializer;
import com.google.common.base.Predicate;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.FluentWait;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LoginPageIT {

    private static final String PAGE_TITLE = "iQ";
    private static WebDriver driver;

    @BeforeClass
    public static void beforeClass() {
        driver = new ChromeDriver();
        driver.manage().timeouts().implicitlyWait(2, TimeUnit.SECONDS);
    }

    @Test
    public void test0010GetLoginPage() {
        LoginPage loginPage = LoginPage.navigateTo(driver, "Dematic");

        assertEquals(PAGE_TITLE, loginPage.getTitle());
    }

    @Test
    public void test0020Login() {
        LoginPage loginPage = LoginPage.navigateTo(driver, "Dematic");

        HomePage homePage = loginPage.login(SecurityInitializer.INSTANCE_ADMIN_USERNAME,
                SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        assertEquals("Welcome to the Home page!", homePage.getWelcome());
    }

    @Test
    public void test0030LoginFailure() {
        LoginPage loginPage = LoginPage.navigateTo(driver, "Dematic");

        LoginPage failedLoginPage = loginPage.loginExpectingFailure("badUserName",
                SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        assertEquals("Unauthorized.", failedLoginPage.getServerError());
    }

    @Test
    public void test0040ValidateLoginPageRenderingWithTenant() {

        Matcher<? super WebElement> defaultCss = new HasCssProperty(LoginPage.THIN_GREY);
        Matcher<? super WebElement> focusCss = new HasCssProperty(LoginPage.THIN_BLUE);
        Matcher<? super WebElement> focusErrorCss = new HasCssProperty(LoginPage.THICK_BLUE);
        Matcher<? super WebElement> clientErrorCss = new HasCssProperty(LoginPage.THICK_GOLD);
        Matcher<? super List<String>> notVisible = Matchers.containsInAnyOrder("Not Visible");
        Matcher<? super WebElement> serverErrorCss = new HasCssProperty(LoginPage.THICK_RED);

        LoginPage loginPage = LoginPage.navigateTo(driver, "Dematic");

        assertThat(loginPage.getTenant(), defaultCss);
        assertThat(loginPage.getUsername(), defaultCss);
        assertThat(loginPage.getPassword(), defaultCss);
        assertEquals("Not Visible", loginPage.getServerError());
        assertThat(loginPage.getClientErrors(), notVisible);

        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.DARK_GREY_BACKGROUND));

        loginPage.clickUsername();
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) focusCss::matches);

        loginPage.clickPassword();
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) focusCss::matches);

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) clientErrorCss::matches);

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Username is required."));

        loginPage.clickUsername();

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Username is required.",
                        "Password is required."));

        loginPage.typeUsername("a");

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) focusCss::matches);

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Username must be at least 5 characters.", "Password is required."));

        loginPage.typePassword("1");

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) focusCss::matches);

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Username must be at least 5 characters.",
                        "Password must be longer than 8 characters."));

        //enter valid (client side) password
        loginPage.typePassword("2345678");

        Matcher<? super List<String>> containsInAnyOrder = Matchers.containsInAnyOrder("Username must be at least 5 characters.");

        new FluentWait<>(loginPage.getClientErrors()).withTimeout(2, TimeUnit.SECONDS)
                .until((Predicate<List<String>>) containsInAnyOrder::matches);

        // This requirement has been updated. Now, the button becomes ready only
        // after ALL attributes are correct.
        loginPage.clickUsername();
        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.DARK_GREY_BACKGROUND));


        //enter valid (client side) username
        loginPage.typeUsername("bcde");

        new FluentWait<>(loginPage.getClientErrors()).withTimeout(2, TimeUnit.SECONDS)
                .until((Predicate<List<String>>) notVisible::matches);

        // This requirement has been updated. Now, the button becomes ready only
        // after ALL attributes are correct.
        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.DARK_BLUE_BACKGROUND));

        //submit login, validate error, validate grey button
        loginPage.clickSignIn();

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getServerErrorDiv())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.LIGHT_GREY_BACKGROUND));

        // Assert that clicking on any input field after a authentication failure DOES NOT reset borders
        loginPage.clickUsername();
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) focusErrorCss::matches);

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        loginPage.clickPassword();
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) focusErrorCss::matches);


        HomePage homePage = loginPage.login(SecurityInitializer.INSTANCE_ADMIN_USERNAME,
                SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        assertEquals("Welcome to the Home page!", homePage.getWelcome());

    }

    @Test
    public void test0042ValidateSubmitWithErrosShowErrorBox() {

        Matcher<? super WebElement> clientErrorCss = new HasCssProperty(LoginPage.THICK_GOLD);

        LoginPage loginPage = LoginPage.navigateTo(driver, "Dematic");

        //submit login, ensure error container has the validation error markup
        loginPage.clickSignIn();

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) clientErrorCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) clientErrorCss::matches);
        getWebElementFluentWait(loginPage.getClientErrorDiv())
                .until((Predicate<WebElement>) clientErrorCss::matches);
     }

    @Test
    public void test0044ValidateButtonDisabled() {

        Matcher<? super WebElement> disabled = new HasAttribute(LoginPage.DISABLED);
        Matcher<? super WebElement> serverErrorCss = new HasCssProperty(LoginPage.THICK_RED);

        LoginPage loginPage = LoginPage.navigateTo(driver, "Dematic");

        //enter valid (client side) username
        loginPage = loginPage.loginExpectingFailure("invalid_user", "invalid_password");

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getServerErrorDiv())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getSignInButton())
                .until((Predicate<WebElement>) disabled::matches);

    }

    @Test
    public void test0050ValidateLoginPageRenderingWithoutTenant() {

        Matcher<? super WebElement> defaultCss = new HasCssProperty(LoginPage.THIN_GREY);
        Matcher<? super WebElement> focusCss = new HasCssProperty(LoginPage.THIN_BLUE);
        Matcher<? super WebElement> focusErrorCss = new HasCssProperty(LoginPage.THICK_BLUE);
        Matcher<? super WebElement> clientErrorCss = new HasCssProperty(LoginPage.THICK_GOLD);
        Matcher<? super List<String>> notVisible = Matchers.containsInAnyOrder("Not Visible");
        Matcher<? super WebElement> serverErrorCss = new HasCssProperty(LoginPage.THICK_RED);

        LoginPage loginPage = LoginPage.navigateTo(driver);

        assertThat(loginPage.getTenant(), defaultCss);
        assertThat(loginPage.getUsername(), defaultCss);
        assertThat(loginPage.getPassword(), defaultCss);
        assertEquals("Not Visible", loginPage.getServerError());
        assertThat(loginPage.getClientErrors(), notVisible);

        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.DARK_GREY_BACKGROUND));


        loginPage.clickTenant();
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) focusCss::matches);

        loginPage.clickPassword();
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) focusCss::matches);

        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) clientErrorCss::matches);

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Tenant is required."));


        loginPage.clickTenant();

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Tenant is required.",
                        "Password is required."));

        // Assert that an invalid tenant name, using a valid username and password fails to authenticate
        loginPage.loginExpectingFailure("a", SecurityInitializer.INSTANCE_ADMIN_USERNAME, SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getServerErrorDiv())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.LIGHT_GREY_BACKGROUND));

        // Assert that clicking on any input field after a authentication failure DOES NOT reset borders
        loginPage.clickTenant();
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) focusErrorCss::matches);
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        loginPage.clickUsername();
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) focusErrorCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        loginPage.clickPassword();
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) focusErrorCss::matches);
        
        // Assert that a keydown on any input field after a authentication failure resets borders
        loginPage.getTenant().sendKeys("a");
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) focusCss::matches);
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) defaultCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) defaultCss::matches);

        loginPage.loginExpectingFailure("a", SecurityInitializer.INSTANCE_ADMIN_USERNAME, SecurityInitializer.INSTANCE_ADMIN_PASSWORD);
        loginPage.getUsername().sendKeys("a");
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) defaultCss::matches);
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) focusCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) defaultCss::matches);

        loginPage.loginExpectingFailure("a", SecurityInitializer.INSTANCE_ADMIN_USERNAME, SecurityInitializer.INSTANCE_ADMIN_PASSWORD);
        loginPage.getPassword().sendKeys("a");
        getWebElementFluentWait(loginPage.getTenant())
                .until((Predicate<WebElement>) defaultCss::matches);
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) defaultCss::matches);
        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) focusCss::matches);

        HomePage homePage = loginPage.login(SecurityInitializer.INSTANCE_TENANT_NAME, SecurityInitializer.INSTANCE_ADMIN_USERNAME,
                SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        assertEquals("Welcome to the Home page!", homePage.getWelcome());
    }

    private static FluentWait<WebElement> getWebElementFluentWait(WebElement webElement) {
        return new FluentWait<>(webElement).withTimeout(2, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void afterClass() {
        driver.close();
        driver.quit();
    }
}