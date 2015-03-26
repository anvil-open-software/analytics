/**
 * Created by silveir on 3/24/15.
 */

angular.module("ResourceServices")
    .factory('TenantResourceSrvc', ['$resource', function($resource) {
        /**
         * tenantSrvc - Service used to manage DLABS tenant resources.
         * Uses AngularJS $ngResource service as a wrapper around $http to return the
         * following object:
         *    { 'get':    {method:'GET'},
         *      'save':   {method:'POST'},
         *      'query':  {method:'GET', isArray:true},
         *      'remove': {method:'DELETE'},
         *      'delete': {method:'DELETE'}
         *    }
         *
         * One additional method was added to support the HTTP PUT method
         *
         * @param {$resource} The AnugularJS resource service,http://docs.angularjs.org/api/ngResource.$resource
         * @author Rodrigo Silveira
         */
        return $resource('resources/tenant/:id', {id: '@id'},
            {
                'update': { method:'PUT' }
            });
    }]
);
