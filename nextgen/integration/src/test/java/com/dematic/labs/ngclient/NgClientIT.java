package com.dematic.labs.ngclient;

import org.junit.*;
import org.junit.runners.MethodSorters;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.net.MalformedURLException;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NgClientIT {

    private static WebDriver driver;
    private static String homePage = "http://127.0.0.1:8080/ngclient/";

    public NgClientIT() { }

    @BeforeClass
    public static void beforeClass() throws MalformedURLException {
        driver = new ChromeDriver();
    }

    @Test
    public void test0000GetLandingPage ()  throws MalformedURLException {
        // Little change to force a build ...
        driver.get(homePage);
        String title = driver.getTitle();
        assertEquals(title.compareTo("ngclient"), 0);
    }

    @Test
    public void test0100Login ()  throws MalformedURLException {
        WebElement username;
        WebElement password;
        WebElement login;
        WebDriverWait wait;
        WebElement hello;
        WebElement welcome;
        //WebElement logout = null;

        driver.get(homePage);

        //username = driver.findElement(By.name("username"));
        //password = driver.findElement(By.name("password"));
        //login = driver.findElement(By.id("log-in"));
        username = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.name("username")));
        password = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.name("password")));
        login    = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.id("log-in")));

        username.clear();
        username.sendKeys("superuser");
        password.clear();
        password.sendKeys("abcd1234");
        login.submit();

        wait = new WebDriverWait(driver, 40);
        hello = wait.until(ExpectedConditions.elementToBeClickable(By.id("hello")));
        Assert.assertEquals(hello.getText().compareTo("Hello superuser from Dematic"), 0);

        wait = new WebDriverWait(driver, 40);
        welcome = wait.until(ExpectedConditions.elementToBeClickable(By.id("welcome")));
        Assert.assertEquals(welcome.getText().compareTo("Welcome to the Home page!"), 0);
    }

    @Test
    public void test0200AuthenticateValidRequest ()  throws MalformedURLException {
        WebElement tenantClick;
        WebElement tenants;
        String tenants_class;

        tenantClick = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.id("tenant-click")));
        tenantClick.click();

        tenants = (new WebDriverWait(driver, 2)).until(ExpectedConditions.visibilityOfElementLocated(By.id("tenants")));
        tenants_class = tenants.getAttribute("class");
        Assert.assertNotNull(tenants_class);
        Assert.assertEquals(tenants_class.contains("ng-hide"), false);
    }

    @AfterClass
    public static void afterClass() {
        driver.close();
        driver.quit();
    }
}
