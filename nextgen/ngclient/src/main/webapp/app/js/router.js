angular.module("app").config(function($routeProvider, $locationProvider) {

  $locationProvider.html5Mode({enabled:true});

    $routeProvider.when('/', {
        //templateUrl: 'login.html',
        templateUrl: 'dlSigninPrompt.html',
        controller: 'LoginController'
    });

  $routeProvider.when('/login', {
    //templateUrl: 'login.html',
    templateUrl: 'dlSigninPrompt.html',
    controller: 'LoginController'
  });

  $routeProvider.when('/home', {
    templateUrl: 'home.html',
    controller: 'HomeController'
  });

  $routeProvider.otherwise({ redirectTo: '/' });

});
