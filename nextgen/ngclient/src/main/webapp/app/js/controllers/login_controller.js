/**
 * The Authentication Controller ...
*/
angular.module('Authentication')

.controller('LoginController', ['$scope', '$location', 'AuthenticationService', 'AuthenticatedUser', 'SecurityToken',
    function($scope, $location, AuthenticationService, AuthenticatedUser, SecurityToken) {
        $scope.title = "Landing";
        //$scope.credentials = { username: "superuser", password: "abcd1234" };
        $scope.credentials = { username: "", password: "" };
        $scope.unauthorized = false;
        $scope.ready = 'dl-not-ready';

        var onLoginSuccess = function() {
            $location.path('/home');
        };

        $scope.login = function() {
            AuthenticationService.login($scope.credentials).success(function(data) {
                AuthenticatedUser.setAuthenticatedUser(data);
                SecurityToken.set(data);
                onLoginSuccess();
            });
        };
    }
]);
