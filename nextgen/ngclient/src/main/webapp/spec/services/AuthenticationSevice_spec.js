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

        it('it has two and only two input elements named username and password', function() {
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

        it('it has one and only one button element of type submit', function() {
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
});
