/**
 * Created by silveir on 3/28/15.
 */


/**
* The Authentication Directives
*/
angular.module('Authentication')
    .directive('dlSigninPrompt', function() {
        return {
            restrict: 'AE',
            // Note: Lineman automatically creates a templateCache!
            templateUrl: 'signin.html'
        };
    })
    .directive('dlSubmit', ['$parse', function($parse) {
        /*
           A simple directive that binds to the form’s submit event, and if the
           ngFormController is not valid, cancels the event. Otherwise it will
           execute the defined expression. This is basically a copy of the angular
           ngSubmit with the addition of a validation check.
         */
        return {
            restrict: 'A',
            require: 'form',
            link: function (scope, formElement, attributes, formController) {

                var fn = $parse(attributes['dlSubmit']);

                formElement.bind('submit', function (event) {
                    // if form is not valid cancel it.
                    if (!formController.$valid) {return false;}

                    scope.$apply(function() {
                        fn(scope, {$event:event});
                    });
                });
            }
        };
    }])
    .directive('dlSignin', function() {
        return {
            restrict: 'A',
            link: function (scope, element, attributes, formController) {
                scope.$on('dl-unauthorized-event', function(event, args) {
                    element.addClass('dl-unauthorized');
                    scope.unauthorized = true;
                });
                element.bind('focus', function() {
                    // This is a classical example where good old jQuery shines.
                    // I need to remove the 'dl-unauthorized' from all DOM elements
                    // that have it. I could not have been done in a simpler way using
                    // angular machinery.
                    $('.dl-unauthorized').removeClass('dl-unauthorized');

                    element.removeClass('dl-blurred');
                    element.addClass('dl-focused');
                    scope.unauthorized = false;
                    scope.$digest();
                });
                element.bind('blur', function() {
                    if (element.hasClass('ng-valid')) {
                        scope.ready='btn-primary';
                    }
                    element.removeClass('dl-focused');
                    element.addClass('dl-blurred');
                    scope.$digest();
                });
            }
        };
    })
    .directive('dlSigninButton', function() {
        return {
            restrict: 'E',
            templateUrl: 'signinButton.html',
            link: function(scope, element, attributes, controller) {
            }
        };
    })
    .directive('dlRemoveHover', ['$compile', function($compile) {
        return {
            restrict: 'A',
            link: function(scope, formElement, attributes, controller) {
                scope.$on('dl-unauthorized-event', function(event, args) {
                    // At this point we want to reset the sign-in button to its
                    // original state. This would be easy, except for ::hover
                    // attribute which cannot be easily reset. To do so we will
                    // replace the current sign-in button with the original one.
                    var signinButton = formElement.find('button');
                    if (signinButton) {
                        // Found the darn thing
                        var parent = angular.element(signinButton).parent();
                        if (parent) {
                            // Found the parent. Remove exiting button, insert new one.
                            scope.ready = 'dl-not-ready';
                            var signinButtoNnew = angular.element(document.createElement('dl-signin-button'));
                            $compile( signinButtoNnew )( scope );
                            signinButton.remove();
                            parent.append(signinButtoNnew);
                        }
                    }
                });
            }
        };
    }])
    .directive('dlAuthenticationSpinner', function() {
        return {
            restrict: 'A',
            link: function(scope, element) {
                scope.$on('dl-authentication-start', function () {
                    scope.showSpinner=true;
                });
                scope.$on('dl-authentication-end', function () {
                    scope.showSpinner=false;
                });
            }
        };
    });
