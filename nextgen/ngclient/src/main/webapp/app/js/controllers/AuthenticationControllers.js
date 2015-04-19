/**
 * Authentication Module
 */
angular.module('Authentication')

    .controller('SigninController', ['$scope', '$location', '$routeParams', 'AuthenticationService', 'AuthenticatedUser', 'SecurityToken',
        function($scope, $location, $routeParams, AuthenticationService, AuthenticatedUser, SecurityToken) {
            $scope.title = "Landing";
            //$scope.credentials = { username: "superuser", password: "abcd1234" };
            $scope.credentials = { tenant: null, username: "", password: "" };
            $scope.unauthorized = false;
            $scope.ready = 'dl-not-ready';
            $scope.showSpinner=false;
            $scope.credentials.tenant = $routeParams.hasOwnProperty('tenant') ? $routeParams['tenant'] : null;
            //$scope.tenantReadonly = $scope.credentials.tenant ? true : false;
            $scope.urlHasTenant = $scope.credentials.tenant ? true : false;
            $scope.authorizing = false;

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
