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
    });
