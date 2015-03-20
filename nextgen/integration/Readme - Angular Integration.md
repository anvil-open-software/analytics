# Enabling AngularJS Integration Tests
The AngularJS Integration tests goal is to ensure that each and all REST endpoints perform as required. Although AngularJS offers a very rich framework for [integration tests](https://docs.angularjs.org/guide/e2e-testing), we will start using a [Selenium / Webdriver / Java](http://docs.seleniumhq.org/docs/01_introducing_selenium.jsp) framework  due to its ability to easily integrated our Jenkis-based CI environment. Later on we will consider adding the [AngularJS end to end](https://docs.angularjs.org/guide/e2e-testing) capability. Please note that, although this is not part of this document, we will start using the [AngularJS unit testing](https://docs.angularjs.org/guide/unit-testing) immediately.

Of interest to this discuss are the steps required for you to set up and run the AngularJS Integration tests in _**IntelliJ**_ and _**Cargo**_.

# Selenium / Webdriver / Java Testing Architecture
As it will be seen later, this architecture imposes the requirement for a number of careful steps to be taken to support tests to be executed. Therefore we will include a few relevant details herein to aid in the subsequent explanations. The architecture consists of the following fundamental elements:

 1. Application under test
 1. Tests
 1. Selenium using WebDriver)
 1. The browser interface
 1. The browser

## Application under test
This represents the nextgen client being tested, hereon referred to as _**ngclient**_, as in _**AngularJS**_ Client.

## Tests
This represents the universe of tests used to validate _**ngclient**_. These tests are located in _**nexgen/ingration/src/test/java/com.dematic.labs/ngclient**_, with only one class, _**NgClientIT**_, presently being used to harness the AngularJS integration tests.

## Selenium WebDriver
[Selenium WebDriver]((http://docs.seleniumhq.org/docs/01_introducing_selenium.jsp)) is a tool for automating testing web applications, and in particular to verify that they work as expected. It's not tied to any particular test framework, so it can be used equally well with JUnit, TestNG or from a plain old "main" method.

WebDriver is the name of the key interface against which tests should be written, but there are [several implementations](https://code.google.com/p/selenium/wiki/NextSteps).
In our case we have selected to start our work using the Chrome browser (see comments on _**HtmlUnit**_ below):

```java
    @BeforeClass
    public static void beforeClass() throws MalformedURLException {
        driver = new ChromeDriver();
    }
```
**Note on HtmlUnit**: It is enticing to use _**HtmlUnit**_ since it does not require a browser at all. Unfortunately it was not be for me this time around. I made an attempt to use the _**HtmlUnit**_ interface implementation but was not successful due to an apparent [incompatibility between _**HtmlUnit**_ and AngularJS](http://stackoverflow.com/questions/20153104/htmlunit-not-working-with-angularjs) associated with AngularJS use of the document.querySelectorAll() when it boots. After spending a few houes attemping recommendations found on a few blogs, I gave up; some brave or smarter sould might consider to do this later on.

## Talking to the browser
[Selenium WebDriver]((http://docs.seleniumhq.org/docs/01_introducing_selenium.jsp)) is an open source tool for automated testing of webapps across many browsers. It provides capabilities for navigating to web pages, user input, JavaScript execution, and more. All implementations of WebDriver that communicate with the browser, or a RemoteWebDriver, server shall use a common [wire protocol](https://code.google.com/p/selenium/wiki/JsonWireProtocol#Introduction). This wire protocol defines a RESTful web service using JSON over HTTP.

As indicated, given the narrow purposes of our first tests, we will focus on only one broweser, Google Chrome, and use Chromedriver](https://code.google.com/p/selenium/wiki/ChromeDriver) was wire protocol implementation to driver the Google Chrome browser.

See this [document](https://sites.google.com/a/chromium.org/chromedriver/getting-started) for instructions on how to install Chromedriver. For simplicity sake, please:
1. Ensure that the _**/usr/bin**_ folder is in your _**PATH**_.
2. Ensure that Chromedriver is installed in _**/usr/bin**_ or that you have a _**/usr/bin/Chromedriver**_ symbolic link to the place where you installed Chromedriver.

## The browser
Except for the browser-less Webdriver interfaceas, HtmlUnit and PhantonJS, you will need to have a the Google Chrome browser installed in the folder natural to the environment in which it is being tested, as indicated in [this document](https://code.google.com/p/selenium/wiki/ChromeDriver). Again, for simplicity sake, please:

1. Ensure that Google Chrome is installed in the folder natural to the environment. In the case you install it anywhere else ensure that:
 - The _**/usr/bin**_ folder is in your _**PATH**_.
 - That you have a _**/usr/bin/google-chrome**_ symbolic link to the place where you installed Google Chrome.

# Running AngularJS Integration Tests in IntelliJ
The AngularJS integration tests are grouped in classes residing the _**nextgen/integration**_ module's _**src/test**_ folder, in the _**com.dematic.labs.ngclient**_ package.

The _**NgClientIT**_ class is the first, and very rudimentary, class used to perform integration tests. Its sole purpose is to validate that the AngularJS Integration framework works. As the actual tests are written we will evolve the framework to abstract common tasks and have the ability to evaluate browsers other than Chrome.

To run the AngularJS Integration Tests in IntelliJ:

1. Please see the instructions to install _**Chromedriver**_ and _**Google Chrome**_ above.
1. Open the IntelliJ Project window
1. Navigate to nexgen/ingration/src/test/java/com.dematic.labs/ngclient
1. Right click on NgClient
1. Select Run NgClientIT

You show see the results shown in your Run NgClient window.

# Running Integration Tests in Cargo
The steps to run the AngularJS Integration Tests in Cargo (when executing the _**mvn clean install**_ command) is broken into:

 1. Steps to run in your sandbox environment
 1. Steps to run in the build environment

Before going into the details of these steps, a few words about the
## Steps to run in your sandbox environment
1. Please see the instructions to install _**Chromedriver**_ and _**Google Chrome**_ above.
1. run _**mvn**_
2. 
## Steps to run in the build environment
1. Please see the instructions to install _**Chromedriver**_ and _**Google Chrome**_ above.
