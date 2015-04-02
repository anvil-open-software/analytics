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
                    scope.unauthorized = false;
                    scope.$digest();
                });
                element.bind('blur', function() {
                    element.addClass('dl-blurred');
                    if (element.hasClass('ng-valid')) {
                       scope.ready='btn-primary';
                    }
                });
            }
        };
    });
