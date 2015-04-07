
//var protractor = require('../node_modules/protractor/lib/protractor.js');
describe('ngclient', function() {
    describe("index", function () {
        var ptor;

        beforeEach(function() {
            //ptor = protractor.getInstance();
            browser.get('http://localhost:8080/ngclient/');
        });

        var form,
            username,
            password,
            signin;

        var isThinGreyBoarder = function(element) {
            var valid = false;
            element.getCssValue('border-bottom-color').then(function(cssValue) {
                console.log('cssValue: ' + cssValue);
            });
            return valid;
        };

        describe('As a User I want to have access to a Log In form including:', function() {
            it('an form element named signin-form', function() {
                var xpath = "//form[@name='signin-form']";
                form = element(by.xpath(xpath));
                expect(form.isPresent()).toBeTruthy();
                expect(form.isDisplayed()).toBeTruthy();
            });
            it('an input element named username', function() {
                var xpath = "//input[@name='username']";
                username = element(by.xpath(xpath));
                expect(username.isPresent()).toBeTruthy();
                expect(username.isDisplayed()).toBeTruthy();
            });
           it('an input element named password', function() {
                var xpath = "//input[@name='password']";
                password = element(by.xpath(xpath));
                expect(password.isPresent()).toBeTruthy();
                expect(password.isDisplayed()).toBeTruthy();
            });
            it('a button element named signin', function() {
                var xpath = "//button[@name='signin']";
                var signin = element(by.xpath(xpath));
                expect(signin.isPresent()).toBeTruthy();
                expect(signin.isDisplayed()).toBeTruthy();
            });
        });
        describe('As a User I want a the input boxes rendered:', function() {
            describe('with a thin grey border when originally rendered:', function() {
                it('input username', function() {
                    var borderOK = isThinGreyBoarder(username);
                    expect(borderOK).toBe(true);
                });
                it('input password', function() {
                    expect(false).toBe(true);
                });
            });
            describe('with a thin blue border when focused:', function() {
                it('input username', function() {
                    expect(false).toBe(true);
                });
                it('input password', function() {
                    expect(false).toBe(true);
                });
            });
            describe('with a thin grey border when touched and valid:', function() {
                it('input username is between 5 and 30 characters', function() {
                    expect(false).toBe(true);
                });
                it('input password is between 8 and 30 characters', function() {
                    expect(false).toBe(true);
                });
             });
            describe('with a thick gold border when touched and invalid:', function() {
                it('input username is shorter than 5 characters', function() {
                    expect(false).toBe(true);
                });
                it('input username is longer than 30 characters', function() {
                    expect(false).toBe(true);
                });
                it('input password is shorter than 8 characters', function() {
                    expect(false).toBe(true);
                });
                it('input password is longer than 30 characters', function() {
                    expect(false).toBe(true);
                });
            });
            describe('with a thick red border when touched and authentication fails:', function() {
                it('input username is invalid', function() {
                    expect(false).toBe(true);
                });
                it('input password is invalid', function() {
                    expect(false).toBe(true);
                });
                it('input username is invalid and input password is invalid', function() {
                    expect(false).toBe(true);
                });
             });
        });
        describe('As a User I want a the error message  boxes rendered:', function() {
            describe('with a thick gold border when at least one of the input elements is invalid:', function() {
                it('input username is shorter than 5 characters', function() {
                    expect(false).toBe(true);
                });
                it('input username is longer than 30 characters', function() {
                    expect(false).toBe(true);
                });
                it('input password is shorter than 8 characters', function() {
                    expect(false).toBe(true);
                });
                it('input password is longer than 30 characters', function() {
                    expect(false).toBe(true);
                });
                it('input username is shorter than 5 characters and password is shorter than 8 characters', function() {
                    expect(false).toBe(true);
                });
                it('input username is shorter than 5 characters and password is longer than 30 characters', function() {
                    expect(false).toBe(true);
                });
                it('input username is longer than 30 characters and password is shorter than 8 characters', function() {
                    expect(false).toBe(true);
                });
                it('input username is longer than 30 characters and password is longer than 30 characters', function() {
                    expect(false).toBe(true);
                });
            })
            describe('with a thick gold border when at least one of the input elements is invalid:', function() {
                it('input username is invalid', function() {
                    expect(false).toBe(true);
                });
                it('input password is invalid', function() {
                    expect(false).toBe(true);
                });
                it('input username is invalid and input password is invalid', function() {
                    expect(false).toBe(true);
                });
            })
        });
    });
});
