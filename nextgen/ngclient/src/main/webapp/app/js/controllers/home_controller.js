/**
 * Home Controller
 */

angular.module('Authentication')

.controller('HomeController', ['$scope', '$location', 'AuthenticatedUser', 'TenantResourceSrvc',
    function($scope, $location, AuthenticatedUser, TenantResourceSrvc) {
        $scope.title = "Home";
        $scope.message = "Mouse Over these images to see a directive at work";
        $scope.authenticatedUser = AuthenticatedUser.getAuthenticatedUser();
        //$scope.tenants = [{name: 'Dematic'}, {name: 'Safeway'}, {name: 'Wallmart'}, {name: 'UPS'}];
        $scope.tenants = [];
        $scope.showDetails = false;

        var onLogoutSuccess = function (response) {
            $location.path('/login');
        };

        $scope.showTenants = function(event) {
            $scope.showDetails = !$scope.showDetails;
            $scope.tenants = $scope.showDetails ? TenantResourceSrvc.query() : [];
        };

        $scope.logout = function () {
            onLogoutSuccess();
        };
    }
]);
