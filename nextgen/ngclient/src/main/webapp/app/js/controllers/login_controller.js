/**
 * The Authentication Controller
*/
angular.module('Authentication')

.controller('LoginController', function($scope, $location, AuthenticationService) {
    $scope.title = "Landing";
    $scope.credentials = { username: "", password: "" };

    var onLoginSuccess = function() {
        $location.path('/home');
    };

    $scope.login = function() {
        AuthenticationService.login($scope.credentials).success(onLoginSuccess);
    };
});
