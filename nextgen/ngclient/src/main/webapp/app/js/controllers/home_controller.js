/**
 * Home Controller
 */

angular.module('Authentication')

.controller('HomeController', ['$scope', '$location', 'AuthenticationService', 'AuthenticatedUser',
    function($scope, $location, AuthenticationService, AuthenticatedUser) {
        $scope.title = "Home";
        $scope.message = "Mouse Over these images to see a directive at work";
        $scope.authenticatedUser = AuthenticatedUser.getAuthenticatedUser();
        $scope.tenants = [{name: 'Dematic'}, {name: 'Safeway'}, {name: 'Wallmart'}, {name: 'UPS'}];

        var onLogoutSuccess = function (response) {
            $location.path('/login');
        };

        $scope.logout = function () {
            //AuthenticationService.logout().success(onLogoutSuccess);
            onLogoutSuccess();
        };
    }
]);
