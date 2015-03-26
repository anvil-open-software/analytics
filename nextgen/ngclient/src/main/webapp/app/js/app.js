angular.module('Authentication', []);
angular.module('ResourceServices', []);
angular.module('SecurityServices', []);

angular.module("app",
	[
		'ngResource',
		'ngRoute',
        'ResourceServices',
        'Authentication',
        'SecurityServices'
	]
).run(function($rootScope) {
  // adds some basic utilities to the $rootScope for debugging purposes
		$rootScope.log = function(thing) {
			console.log(thing);
		};

		$rootScope.alert = function(thing) {
            // comment
			alert(thing);
		};
	}
);
