/**
 * The Authentication Controller
*/
angular.module('Authentication')

.controller('LoginController', ['$scope', '$location', 'AuthenticationService', 'AuthenticatedUser',
    function($scope, $location, AuthenticationService, AuthenticatedUser) {
        $scope.title = "Landing";
        $scope.credentials = { username: "", password: "" };

        var onLoginSuccess = function() {
            $location.path('/home');
        };

        $scope.login = function() {
            AuthenticationService.login($scope.credentials).success(function(data) {
                AuthenticatedUser.setAuthenticatedUser(data);
                onLoginSuccess();
            });
        };
    }
]);
