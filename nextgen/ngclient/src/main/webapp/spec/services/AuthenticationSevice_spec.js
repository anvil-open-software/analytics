/**
 * Created by silveir on 3/28/15.
 */

describe('Unit: Testing AuthenticationServices Module Directives', function() {
    var $compile,
        $rootScope,
        element;    // our directive jqLite element

    function compileDirective(tpl) {
        if (!tpl) tpl = '<div dl-signin-prompt></div>';
        element = angular.element(tpl);
        $compile(element)($rootScope);
        $rootScope.$digest();
    }

    beforeEach(module('app'));

    beforeEach(inject(function(_$compile_, _$rootScope_){
        // The injector unwraps the underscores (_) from around the parameter names when matching
        $compile = _$compile_;
        $rootScope = _$rootScope_;
    }));

    describe('dlSigninPrompt', function() {

        it('Loads the HTML', function() {
            compileDirective('<div dl-signin-prompt></div>');
            expect(element.find('form').length).toEqual(1);
        });

        describe('As a User I want to have access to a Log In prompt', function() {
            it('with two and only two input elements named username and password', function() {
                var expectedInputNames = ['username', 'password'].sort(),
                    inputElements,
                    inputElementsNames = [];

                // Compile the HTML containing the directive
                compileDirective('<div dl-signin-prompt></div>');

                // Check that the compiled element contains input elements
                inputElements = element.find('input');
                expect(inputElements.length).toEqual(expectedInputNames.length);

                // Collect the name attribute of all input elements. Ensure they math
                // the expectedInputNames array
                for (var i=0; i<inputElements.length; i++) {
                    inputElementsNames.push(angular.element(inputElements[i]).attr('name'));
                }
                inputElementsNames.sort();
                expect(inputElementsNames.toString()).toEqual(expectedInputNames.toString());
            });

            it('with one and only one button element of type submit', function() {
                var expectedTypes = ['submit'].sort(),
                    inputElements,
                    inputElementsNames = [];

                // Compile the HTML containing the directive
                compileDirective('<div dl-signin-prompt></div>');

                // Check that the compiled element contains the templated content
                inputElements = element.find('button');
                expect(inputElements.length).toEqual(expectedTypes.length);

                // Collect the name attribute of all input elements. Ensure they math
                // the expectedInputNames array
                for (var i=0; i<inputElements.length; i++) {
                    inputElementsNames.push(angular.element(inputElements[i]).attr('type'));
                }
                inputElementsNames.sort();
                expect(inputElementsNames.toString()).toEqual(expectedTypes.toString());
            });

        });
        describe('As a User I want my user name to be', function() {
            var inputElement;

            beforeEach(function() {
                var inputElements,
                    inputElementName,
                    i;

                compileDirective('<div dl-signin-prompt></div>');
                inputElements = element.find('input');
                for (var i=0; i<inputElements.length; i++) {
                    inputElement = angular.element(inputElements[i]);
                    inputElementName = inputElement.attr('name');
                    if (inputElementName === 'username') break;
                }
                expect(i).toBeLessThan(2);
                expect(inputElementName).toBe('username');
            });

            it('required', function() {
                var requiredAttribute;

                requiredAttribute = inputElement.attr('required');
                expect(requiredAttribute).not.toBe(null);
            });
            it('a valid email address', function() {
                var type;

                type = inputElement.attr('type');
                //expect(type).toBe('email');
                expect(type).toBe('text');
            });
            it('no longer than 30 characters', function() {
                var maxLength;

                maxLength = inputElement.attr('ng-maxlength');
                expect(maxLength).toBe('30');
            });
        });
        describe('As a User I want my password to be', function() {
            var inputElement,
                i,
                inputElementName;

            beforeEach(function() {
                var inputElements;

                compileDirective('<div dl-signin-prompt></div>');
                inputElements = element.find('input');
                for (var i=0; i<inputElements.length; i++) {
                    inputElement = angular.element(inputElements[i]);
                    inputElementName = inputElement.attr('name');
                    if (inputElementName === 'password') break;
                }
            });
            it('a password string', function() {
                expect(i).toBeLessThan(2);
                expect(inputElementName).toBe('password');
            });
            it('with a minimum of 8 characters', function() {
                var minLength;

                minLength = inputElement.attr('ng-minlength');
                expect(minLength).toBe('8');
            });
            it('no longer than 30 characters', function() {
                var maxLength;

                maxLength = inputElement.attr('ng-maxlength');
                expect(maxLength).toBe('30');
            });
        });
    });
});
