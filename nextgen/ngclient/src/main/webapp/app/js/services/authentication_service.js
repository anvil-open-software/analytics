/**
 * Authentication
 */
angular.module("Authentication")

 .factory('AuthenticationService', function($http) {
  // these routes map to stubbed API endpoints in config/server.js
  return {
    login: function(credentials) {
        var request = {
            method: 'GET',
            url: 'resources/token',
            headers: {
                "Accept": "application/json",
                "Authorization": "DLabsU " + "Dematic" + ":" + credentials.username + ":" + credentials.password
            }
        };

      return $http(request);
    },
    logout: function() {
      return $http.post('/logout');
    }
  };

});
