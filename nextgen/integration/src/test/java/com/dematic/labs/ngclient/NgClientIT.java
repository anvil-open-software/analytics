package com.dematic.labs.ngclient;

import org.junit.*;
import org.junit.runners.MethodSorters;

import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.net.MalformedURLException;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NgClientIT {

    private static WebDriver driver;

    public NgClientIT() { }

    @BeforeClass
    public static void before() throws MalformedURLException {
        //driver = new HtmlUnitDriver();

        ChromeOptions options = new ChromeOptions();
        options.setBinary("/usr/bin");
        driver = new ChromeDriver();

    }

    @Test
    public void test1GetLandingPage ()  throws MalformedURLException {
        driver.get("http://127.0.0.1:8080/ngclient/");
        System.out.print("Title: " + driver.getTitle());
        assertTrue(driver.getTitle().startsWith("ngclient"));
    }

    @Test
    public void test1Name() throws Exception {

    }

    @Test
    public void test2Login ()  throws MalformedURLException {
        WebElement username = null;
        WebElement password = null;
        WebElement login = null;
        WebDriverWait wait;
        WebElement hello;
        WebElement welcome;
        //WebElement logout = null;

        driver.get("http://127.0.0.1:8080/ngclient/");
        try {
            username = driver.findElement(By.name("username"));
        }
        catch (NoSuchElementException e) {
            fail("Did not find element named username");
        }
        try {
            password = driver.findElement(By.name("password"));
        }
        catch (NoSuchElementException e) {
            fail("Did not find element named password");
        }
        try {
            login = driver.findElement(By.id("log-in"));
        }
        catch (NoSuchElementException e) {
            fail("Did not find element with id log-in");
        }

        username.sendKeys("superuser");
        password.sendKeys("abcd1234");
        login.submit();

        wait = new WebDriverWait(driver, 40);
        hello = wait.until(ExpectedConditions.elementToBeClickable(By.id("hello")));
        Assert.assertEquals(hello.getText().compareTo("Hello superuser from Dematic"), 0);

        wait = new WebDriverWait(driver, 40);
        welcome = wait.until(ExpectedConditions.elementToBeClickable(By.id("welcome")));
        Assert.assertEquals(welcome.getText().compareTo("Welcome to the Home page!"), 0);

    }

    @AfterClass
    public static void after() {
        driver.close();
        driver.quit();
    }
}
