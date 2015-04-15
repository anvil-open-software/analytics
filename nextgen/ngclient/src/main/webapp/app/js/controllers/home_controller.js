/**
 * Home Controller
 */

angular.module('Authentication')

.controller('HomeController', ['$scope', '$location', 'AuthenticatedUser', 'TenantResourceSrvc',
    function($scope, $location, AuthenticatedUser, TenantResourceSrvc) {
        $scope.title = "Home";
        $scope.message = "Mouse Over these images to see a directive at work";
        $scope.authenticatedUser = AuthenticatedUser.getAuthenticatedUser();
        $scope.tenants = {};
        $scope.showDetails = false;

        var onLogoutSuccess = function (response) {
            $location.path('/');
        };

        $scope.showTenants = function(event) {
            if (!$scope.showDetails) {
                TenantResourceSrvc.get().$promise.then(function(tenants){
                    $scope.tenants = tenants;
                    $scope.showDetails = !$scope.showDetails;
                });
            }
            else {
                $scope.showDetails = !$scope.showDetails;
                $scope.tenants = {};
            }
        };

        $scope.logout = function () {
            onLogoutSuccess();
        };
    }
]);
