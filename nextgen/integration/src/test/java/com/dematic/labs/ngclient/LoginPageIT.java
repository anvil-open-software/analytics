package com.dematic.labs.ngclient;

import com.dematic.labs.ngclient.page.HasCssProperty;
import com.dematic.labs.ngclient.page.HomePage;
import com.dematic.labs.ngclient.page.LoginPage;
import com.dematic.labs.picketlink.SecurityInitializer;
import com.dematic.labs.rest.SecuredEndpointHelper;
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

    private static WebDriver driver;

    @BeforeClass
    public static void beforeClass() {
        driver = new ChromeDriver();
        driver.manage().timeouts().implicitlyWait(2, TimeUnit.SECONDS);
    }

    @Test
    public void test0010GetLoginPage() {
        LoginPage loginPage = LoginPage.navigateTo(driver);

        assertEquals(SecuredEndpointHelper.PAGE_TITLE, loginPage.getTitle());
    }

    @Test
    public void test0020Login() {
        LoginPage loginPage = LoginPage.navigateTo(driver);

        HomePage homePage = loginPage.login(SecurityInitializer.INSTANCE_ADMIN_USERNAME,
                SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        assertEquals("Welcome to the Home page!", homePage.getWelcome());
    }

    @Test
    public void test0030LoginFailure() {
        LoginPage loginPage = LoginPage.navigateTo(driver);

        LoginPage failedLoginPage = loginPage.loginExpectingFailure("badUserName",
                SecurityInitializer.INSTANCE_ADMIN_PASSWORD);

        assertEquals("Unauthorized.", failedLoginPage.getServerError());
    }

    @Test
    public void test0040ValidateLoginPageRendering() {

        Matcher<? super WebElement> defaultCss = new HasCssProperty(LoginPage.THIN_GREY);
        Matcher<? super WebElement> focusCss = new HasCssProperty(LoginPage.THIN_BLUE);
        Matcher<? super WebElement> clientErrorCss = new HasCssProperty(LoginPage.THICK_GOLD);
        Matcher<? super List<String>> notVisible = Matchers.containsInAnyOrder("Not Visible");
        Matcher<? super WebElement> serverErrorCss = new HasCssProperty(LoginPage.THICK_RED);

        LoginPage loginPage = LoginPage.navigateTo(driver);

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
                Matchers.containsInAnyOrder("Username is required...."));

        loginPage.clickUsername();

        assertThat(loginPage.getClientErrors(),
                Matchers.containsInAnyOrder("Username is required....",
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

        loginPage.clickUsername();
        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.DARK_BLUE_BACKGROUND));

        //enter valid (client side) username
        loginPage.typeUsername("bcde");

        new FluentWait<>(loginPage.getClientErrors()).withTimeout(2, TimeUnit.SECONDS)
                .until((Predicate<List<String>>) notVisible::matches);

        //submit login, validate error, validate grey button
        loginPage.clickSignIn();

        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        getWebElementFluentWait(loginPage.getServerErrorDiv())
                .until((Predicate<WebElement>) serverErrorCss::matches);

        assertThat(loginPage.getSignInButton(), new HasCssProperty(LoginPage.LIGHT_GREY_BACKGROUND));

        loginPage.clickUsername();
        getWebElementFluentWait(loginPage.getUsername())
                .until((Predicate<WebElement>) focusCss::matches);

        getWebElementFluentWait(loginPage.getPassword())
                .until((Predicate<WebElement>) defaultCss::matches);


        HomePage homePage = loginPage.login(SecurityInitializer.INSTANCE_ADMIN_USERNAME,
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
