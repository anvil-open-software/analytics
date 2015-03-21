# Enabling AngularJS Integration Tests
The AngularJS Integration tests goal is to ensure that each and all REST endpoints perform as required. Although AngularJS offers a very rich framework for [integration tests](https://docs.angularjs.org/guide/e2e-testing), we will start using a [Selenium / Webdriver / Java](http://docs.seleniumhq.org/docs/01_introducing_selenium.jsp) framework  due to its ability to easily integrated our Jenkis-based CI environment. Later on we will consider adding the [AngularJS end to end](https://docs.angularjs.org/guide/e2e-testing) capability. Please note that, although this is not part of this document, we will start using the [AngularJS unit testing](https://docs.angularjs.org/guide/unit-testing) immediately.

Of interest to this discuss are the steps required for you to set up and run the AngularJS Integration tests in _**IntelliJ**_ and _**Cargo**_.

# Selenium / Webdriver / Java Testing Architecture
As it will be seen later, this architecture imposes the requirement for a number of careful steps to be taken to support tests to be executed. Therefore we will include a few relevant details herein to aid in the subsequent explanations. The architecture consists of the following fundamental elements:

 1. Application under test
 1. Tests
 1. Selenium WebDriver
 1. Selenium Browser Bridge
 1. Browser
 1. X virtual framebuffer 

## Application under test
This represents the nextgen client being tested, hereon referred to as _**ngclient**_, as in _**AngularJS**_ Client.

## Tests
This represents the universe of tests used to validate _**ngclient**_. These tests are located in _**nexgen/ingration/src/test/java/com.dematic.labs/ngclient**_,; at the moment there is only one class, _**NgClientIT**_, being used to harness the AngularJS integration tests.

## Selenium WebDriver
[Selenium WebDriver]((http://docs.seleniumhq.org/docs/01_introducing_selenium.jsp)) is a tool for automating testing web applications, and in particular to verify that they work as expected. It's not tied to any particular test framework, so it can be used equally well with JUnit, TestNG or from a plain old "main" method.

WebDriver is the name of the key interface against which tests must be written. Although there are [several implementations](https://code.google.com/p/selenium/wiki/NextSteps), in our case we have selected to start our work using the Chrome browser.

One of the implementations, [HtmlUnit](https://code.google.com/p/selenium/wiki/HtmlUnitDriver), is very enticing since it does not require a browser at all. Unfortunately it was not be for me this time around. I made an attempt to use the _**HtmlUnit**_ interface implementation but was not successful due to an apparent [incompatibility between _**HtmlUnit**_ and AngularJS](http://stackoverflow.com/questions/20153104/htmlunit-not-working-with-angularjs) associated with AngularJS use of the document.querySelectorAll() when it boots. After spending a few houes attemping recommendations found on a few blogs, I gave up; some brave or smarter sould might consider to do this later on.

## Selenium Bowser Bridge
[Selenium WebDriver]((http://docs.seleniumhq.org/docs/01_introducing_selenium.jsp)) is an open source tool for automated testing of webapps across many browsers. It provides capabilities for navigating to web pages, user input, JavaScript execution, and more. All implementations of WebDriver that communicate with the browser, or a RemoteWebDriver, server shall use a common [wire protocol](https://code.google.com/p/selenium/wiki/JsonWireProtocol#Introduction). This wire protocol defines a RESTful web service using JSON over HTTP.

As indicated, given the narrow purposes of our first tests, we will focus on only one broweser, Google Chrome, and use Chromedriver](https://code.google.com/p/selenium/wiki/ChromeDriver) was wire protocol implementation to driver the Google Chrome browser.

## Browser
Except for the browser-less Webdriver interfaceas, [HtmlUnit](https://code.google.com/p/selenium/wiki/HtmlUnitDriver) and [PhantonJS](http://phantomjs.org/), you will need to have a browser installed in order to run Selenium WebDriver tests. We will use Google Chrome which must be installed in the folder natural to the environment in which it is being tested, as indicated in [this document](https://code.google.com/p/selenium/wiki/ChromeDriver). 

## X virtual framebuffer
X virtual framebuffer, hereon referred to as _**Xvfb**_, is a display server implementing the X11 display server protocol. In contrast to other display servers Xvfb performs all graphical operations in memory without showing any screen output. This is very convinient when running Selenium WebDriver on a build environment without an installed display.

# Installing components
## Application under test
There are no additiounal installation requirements for the purpose of testing it.

## Node
The _**ngClient**_ development environment depends heavily on [Node.JS](https://nodejs.org/). If you have not done so, please navigate to the [Node.JS](https://nodejs.org/) and install it on your workstation. For additional details, please refer to [TN-007  Nextgen Client Side Development Framework](http://sdrn034d/confluence/display/DLAB/TN-007+Nextgen+Client+Side+Development+Framework)

## Tests
There are no additiounal installation requirements for the purpose of running the tests.

## Selenium WebDriver
It comes as part of the Selenium WebDriber libraries, defined as a dependency in the _**integration**_ module _**pom.xml**_:
```xml
        <dependency>
            <groupId>org.seleniumhq.selenium</groupId>
            <artifactId>selenium-java</artifactId>
            <version>2.45.0</version>
            <scope>test</scope>
        </dependency>
```

## Selenium Browser Bridge
You will have to download and install your own. Since we are focusing on Google Chrome for the time being, we will use Chromedriver](https://code.google.com/p/selenium/wiki/ChromeDriver); see this [document](https://sites.google.com/a/chromium.org/chromedriver/getting-started) for instructions on how to install _**Chromedriver**_. For simplicity sake, please:
1. Ensure that the _**/usr/bin**_ folder is in your _**PATH**_.
2. Ensure that Chromedriver is installed in _**/usr/bin**_ or that you have a _**/usr/bin/Chromedriver**_ symbolic link to the place where you installed Chromedriver.

One was also installed in the build environment. In addition it was necessary to define a few runtime properites in the _**maven-failsafe-plugin**_ section of the _**nextgen**_ _**pom.xml**_ file:
```xml
    <webdriver.chrome.driver>${webdriver.chrome.driver}</webdriver.chrome.driver>
    <webdriver.chrome.logfile>${webdriver.chrome.logfile}</webdriver.chrome.logfile>
```

Where the _**webdriver.chrome.driver**_ and _**webdriver.chrome.logfile**_ properties are defined in your _**junit.properties**_ file in the _**.m2**_ folder.

## Browser
If you do not have Google Chrome installed in your workstation, please do so. Ensure that Google Chrome is installed in the folder natural to the environment; see this [link](https://code.google.com/p/selenium/wiki/ChromeDriver) for details on Google Chrome installation folders. In the case you install it anywhere else ensure that:
 - The _**/usr/bin**_ folder is in your _**PATH**_.
 - That you have a _**/usr/bin/google-chrome**_ symbolic link to the place where you installed Google Chrome.

A Google Chrome browser was installed in the build machine. See the _**/usr/bin/google-chrome**_ symbolic link for details of its installation.

## X virtual framebuffer 
_**Xvfb**_ is required only for the build environment since it does not have a display nor a display driver installed. There are two steps required to install _**Xvfb**_:

 - Write a script that launches and _**Xvfb**_ to _**/etc/init.d**_
```bash
#! /bin/sh

### BEGIN INIT INFO
# Provides: Xvfb
# Required-Start: $local_fs $remote_fs
# Required-Stop:
# X-Start-Before:
# Default-Start: 2 3 4 5
# Default-Stop:
### END INIT INFO

N=/etc/init.d/Xvfb

set -e

case "$1" in
  start)
/usr/bin/Xvfb :99 -ac -screen 0 1024x768x24 +extension RANDR &
;;
  stop|reload|restart|force-reload)
;;
  *)  
echo "Usage: $N {start|stop|restart|force-reload}" >&2exit 1
;;
esac

exit 0
```
 - Configure _**xvfb**_ to be launched everytime the server is launched:
```bash
$ update-rc.d /etc/init.d/svfb defaults
```
 - Add an environment variable to tell the client the display port used by _**Xvfb**_. This was done by adding the _**selenium_setup.sh**_ bash script in /etc/profile.d
```bash
#! /bin/sh
export DISPLAY=:99
```