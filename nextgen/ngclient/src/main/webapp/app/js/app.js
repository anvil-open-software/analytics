angular.module('Authentication', []);

angular.module("app",
	[
		'Authentication',
		'ngResource', 
		'ngRoute'
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
