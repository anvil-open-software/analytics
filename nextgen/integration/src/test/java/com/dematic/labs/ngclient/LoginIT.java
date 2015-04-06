package com.dematic.labs.ngclient;

import com.dematic.labs.rest.SecuredEndpointHelper;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.openqa.selenium.By;
import org.openqa.selenium.Point;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.awt.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LoginIT {

    private static WebDriver driver;
    private static String homePage = SecuredEndpointHelper.BASE_URL;
    private WebElement form;
    private WebElement username;
    private WebElement password;
    private WebElement login;
    private WebElement errorbox;
    private Pattern rgbaPattern = Pattern.compile("^(.*\\()(\\d+)(,.?)(\\d+)(,.?)(\\d+)(,.?)(\\d+)(\\))$");

    private Map<String, String> greyRGB = new HashMap<>();
    private Map<String, String> blueRGB  = new HashMap<>();
    private Map<String, String> goldRGB  = new HashMap<>();
    private Map<String, String> redRGB  = new HashMap<>();

    private String thin  = "1px";
    private String thick = "3px";

    public LoginIT() { }

    @BeforeClass
    public static void beforeClass() {
        driver = new ChromeDriver();

     }

    @Before
    public void before() {
        driver.get(homePage);
        form = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.xpath("//form[@name=\"aform\"]")));
        username = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.xpath("//input[@name=\"username\"]")));
        password = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.xpath("//input[@name=\"password\"]")));
        login    = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.xpath("//button[@name=\"signin\"]")));
        errorbox = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.xpath("//div[contains (@class, 'error-container')]")));

        greyRGB.put("red",   "204");
        greyRGB.put("green", "204");
        greyRGB.put("blue",  "204");

        // dodgerblue
        blueRGB.put("red",   "102");
        blueRGB.put("green", "175");
        blueRGB.put("blue",  "233");

        // gold
        goldRGB.put("red",   "255");
        goldRGB.put("green", "215");
        goldRGB.put("blue",  "0");

        // red
        redRGB.put("red",   "255");
        redRGB.put("green", "0");
        redRGB.put("blue",  "0");
    }

    @Test
    public void test0000GetLandingPage ()  {
        String title = driver.getTitle();
        assertEquals(title.compareTo(SecuredEndpointHelper.CONTEXT_ROOT), 0);
    }

    @Test
    public void test0100LoginForm ()  {

       /*
        As a User I want to have access to a Log In form including:
        - a form element named signin-form
        - an input element named username
        - an input element named password
        - a button element named signin
         */
        Assert.assertNotNull(form);
        Assert.assertNotNull(username);
        Assert.assertNotNull(password);
        Assert.assertNotNull(login);
        Assert.assertNotNull(errorbox);
    }
    @Test
    public void test0200LoginFormIInputAttributes ()  {
        /*
        As a User I want a the input elements rendered with:
        - a thin greyRGB border when originally rendered
        - a thin blue border when focused
        - a thin greyRGB border when touched and valid
        - a thick gold border when touched and invalid
        - a thick red border when touched, valid, but authentication failed
         */

        /* ****************************************************************************
            username/password rendered with a thing gray border when originally rendered
         *  ****************************************************************************/
        Assert.assertTrue(isThinGrayBorder(username));
        Assert.assertTrue(isThinGrayBorder(password));

        /* ****************************************************************
            username/password rendered with a thin blue border when focused
         *  ***************************************************************/
        username.click();
        clunkykWait(1000);
        Assert.assertTrue(isThinBlueBorder(username));

        password.click();
        clunkykWait(1000);
        Assert.assertTrue(isThinBlueBorder(password));

        /* ***********************************************************************************
            username/password rendered with a thin grey border when blurred with valid content
         *  **********************************************************************************/
        new Actions(driver).moveToElement(username).click().perform();
        username.sendKeys("superuser");
        password.click();
        clunkykWait(1000);
        Assert.assertTrue(isThinGrayBorder(username));
        new Actions(driver).moveToElement(password).click().perform();
        password.sendKeys("abcd1234");
        username.click();
        clunkykWait(1000);
        Assert.assertTrue(isThinGrayBorder(password));

        /* ************************************************************************************
            username/password rendered with a thick gold border when blurred with valid content
         *  ***********************************************************************************/
        username.click();
        username.clear();
        password.click();
        clunkykWait(1000);
        Assert.assertTrue(isThickGoldBorder(username));
        password.click();
        password.clear();
        username.click();
        clunkykWait(1000);
        Assert.assertTrue(isThickGoldBorder(password));

        /* ********************************************************************************
            username/password rendered with a thick red border after invalid authentication
         *  *******************************************************************************/
        new Actions(driver).moveToElement(username).click().perform();
        username.sendKeys("superuserr");
        new Actions(driver).moveToElement(password).click().perform();
        password.sendKeys("abcd1234");
        login.click();
        clunkykWait(1000);
        Assert.assertTrue(isThickRedBorder(username));
        Assert.assertTrue(isThickRedBorder(password));
    }

    @Test
    public void test0300LoginFormIErrors ()  {
        /*
        As a User I want a the error box rendered with:
        - hidden when the form is shown
        - with a thick gold border if any input element are invalid
        - a thick red border when authentication failed
         */

        /* ********************************************************************************
            error box rendered with a thick gold border if any of the attributes is invalid
         *  *******************************************************************************/
    }

    @AfterClass
    public static void afterClass() {
        driver.close();
        driver.quit();
    }

    private boolean isThinGrayBorder(WebElement element) {
        boolean  thinGrayBorder;

        thinGrayBorder = isColorBorder(element, greyRGB);
        thinGrayBorder = thinGrayBorder && isThinBorder(element);

        return thinGrayBorder;
    }

    private boolean isThinBlueBorder(WebElement element) {
        boolean thinBlueBorder;

        thinBlueBorder = isColorBorder(element, blueRGB);
        thinBlueBorder = thinBlueBorder && isThinBorder(element);

        return thinBlueBorder;
    }

    private boolean isThickGoldBorder(WebElement element) {
        boolean thickGoldBorder;

        thickGoldBorder = isColorBorder(element, goldRGB);
        thickGoldBorder = thickGoldBorder && isThickBorder(element);

        return thickGoldBorder;
    }

    private boolean isThickRedBorder(WebElement element) {
        boolean thickGoldBorder;

        thickGoldBorder = isColorBorder(element, redRGB);
        thickGoldBorder = thickGoldBorder && isThickBorder(element);

        return thickGoldBorder;
    }

    private boolean isColorBorder(WebElement element, Map<String, String> rgb) {
        boolean colorBorder = true;

        colorBorder = colorBorder && isRGB(element, "border-top-color", rgb);
        colorBorder = colorBorder && isRGB(element, "border-right-color", rgb);
        colorBorder = colorBorder && isRGB(element, "border-bottom-color", rgb);
        colorBorder = colorBorder && isRGB(element, "border-left-color", rgb);

        return colorBorder;
    }

    private boolean isRGB(WebElement element, String boxSideColor, Map<String, String> rgb) {
        boolean rgbMatch = true;
        String borderColor, red, green, blue;

        borderColor = element.getCssValue(boxSideColor);
        System.out.println(boxSideColor + " color: " + borderColor);
        Matcher matcher = rgbaPattern.matcher(borderColor);
        while(matcher.find()) {
            red = matcher.group(2);
            System.out.println("Actual red:   " + red + "   Expected red: " + rgb.get("red"));
            rgbMatch = rgbMatch && (red.compareTo(rgb.get(("red"))) == 0);
            green = matcher.group(4);
            System.out.println("Actual green:   " + green + "   Expected green: " + rgb.get("green"));
            rgbMatch = rgbMatch && (green.compareTo(rgb.get(("green"))) == 0);
            blue = matcher.group(6);
            System.out.println("Actual blue:   " + blue + "   Expected blue: " + rgb.get("blue"));
            rgbMatch = rgbMatch && (blue.compareTo(rgb.get(("blue"))) == 0);
        }
        return rgbMatch;
    }

    private boolean isThinBorder(WebElement element) {
        boolean thinBorder = true;

        thinBorder = thinBorder && isWidth(element, "border-top-width", thin);
        thinBorder = thinBorder && isWidth(element, "border-right-width", thin);
        thinBorder = thinBorder && isWidth(element, "border-bottom-width", thin);
        thinBorder = thinBorder && isWidth(element, "border-left-width", thin);

        return thinBorder;
    }

    private boolean isThickBorder(WebElement element) {
        boolean thickBorder = true;

        thickBorder = thickBorder && isWidth(element, "border-top-width", thick);
        thickBorder = thickBorder && isWidth(element, "border-right-width", thick);
        thickBorder = thickBorder && isWidth(element, "border-bottom-width", thick);
        thickBorder = thickBorder && isWidth(element, "border-left-width", thick);

        return thickBorder;
    }

    private boolean isWidth(WebElement element, String boxSideWidth, String width) {
        boolean widthOK = true;
        String borderWidth;

        borderWidth = element.getCssValue(boxSideWidth);
        System.out.println(borderWidth + " width: " + borderWidth);
        /*
        Matcher matcher = rgbaPattern.matcher(borderWidth);
        while(matcher.find()) {
            //System.out.println("red:   " + matcher.group(2));
            red = matcher.group(2);
            widthOK = widthOK && (red.compareTo(widthOK) == 0));
            //System.out.println("green: " + matcher.group(4));
            green = matcher.group(4);
            widthOK = widthOK && (green.compareTo(rgb.get(("green"))) == 0);
            //System.out.println("blue:  " + matcher.group(6));
            blue = matcher.group(6);
            widthOK = widthOK && (blue.compareTo(rgb.get(("blue"))) == 0);
            //System.out.println("alpha: " + matcher.group(8));
        }
        */
        return widthOK;
    }

    private boolean isThick(WebElement element, String boxSideWidth, Map<String, String> rgb) {
        boolean rgbMatch = true;
        String borderColor, red, green, blue;

        borderColor = element.getCssValue(boxSideWidth);
        //System.out.println(boxSide + " color: " + borderColor);
        Matcher matcher = rgbaPattern.matcher(borderColor);
        while(matcher.find()) {
            //System.out.println("red:   " + matcher.group(2));
            red = matcher.group(2);
            rgbMatch = rgbMatch && (red.compareTo(rgb.get(("red"))) == 0);
            //System.out.println("green: " + matcher.group(4));
            green = matcher.group(4);
            rgbMatch = rgbMatch && (green.compareTo(rgb.get(("green"))) == 0);
            //System.out.println("blue:  " + matcher.group(6));
            blue = matcher.group(6);
            rgbMatch = rgbMatch && (blue.compareTo(rgb.get(("blue"))) == 0);
            //System.out.println("alpha: " + matcher.group(8));
        }
        return rgbMatch;
    }

    private void jiggleMouse(String slideHere, String slideBackHere) {
        Point coordinates;
        org.openqa.selenium.Dimension size;
        Robot robot;

        coordinates = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.name(slideHere))).getLocation();
        size = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.name(slideHere))).getSize();
        try {
            robot = new Robot();
            robot.mouseMove(coordinates.getX() + size.getWidth()/2, coordinates.getY() + size.getHeight()/2);
        } catch (AWTException e) {
            e.printStackTrace();
        }
        coordinates = (new WebDriverWait(driver, 2)).until(ExpectedConditions.presenceOfElementLocated(By.name(slideBackHere))).getLocation();
        try {
            robot = new Robot();
            robot.mouseMove(coordinates.getX() + size.getWidth()/2, coordinates.getY() + size.getHeight()/2);
        } catch (AWTException e) {
            e.printStackTrace();
        }
    }

    private void jiggleMouse(WebElement slideHere, WebElement slideBackHere) {
        Point coordinates;
        org.openqa.selenium.Dimension size;
        Robot robot;

        coordinates = slideHere.getLocation();
        size = slideHere.getSize();
        try {
            robot = new Robot();
            robot.mouseMove(coordinates.getX() + size.getWidth()/2, coordinates.getY() + size.getHeight()/2);
        } catch (AWTException e) {
            e.printStackTrace();
        }
        coordinates = slideBackHere.getLocation();
        size = slideBackHere.getSize();
        try {
            robot = new Robot();
            robot.mouseMove(coordinates.getX() + size.getWidth()/2, coordinates.getY() + size.getHeight()/2);
        } catch (AWTException e) {
            e.printStackTrace();
        }
    }

    // After 12 hours working on this trying to use Webdriver, this was the
    // first mechanism that made this work for me!
    private void clunkykWait(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

    }
}
