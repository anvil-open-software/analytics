/**
 * Security Services
 */
angular.module("SecurityServices")
.factory('SecurityToken',
    function() {
        var securityToken = {};
        return {
            set: function (loginData) {
                angular.copy(loginData, securityToken);
                return;
            },
            getRealm: function () {
                return securityToken.hasOwnProperty('realm') ? securityToken.realm : null;
            },
            getToken: function () {
                return securityToken.hasOwnProperty('token') ? securityToken.token : null;
            },
            getSignature: function () {
                return securityToken.hasOwnProperty('signatureKey') ? securityToken.signatureKey : null;
            }
        };
    }
)
.factory('StringToSign',
    function() {
        var service = {
                getHttpVerb: function (config) {
                    var httpVerb = '';

                    httpVerb = config.hasOwnProperty('method')  ? config['method']+'\n'  : ''+'\n';
                    return httpVerb;
                },
                getCannonicalHeaders: function (config) {
                    var cannonicalHeaders = '';
                    // Canonical Headers are: Content-MD5, Content-Type, Date
                    // The value is "" if the header is not present.
                    // If the header "x-dlabs-date" is present, it's value is output for "Date"
                    if (config.hasOwnProperty('headers')) {
                        cannonicalHeaders += config['headers'].hasOwnProperty('Content-MD5')  ? config['headers']['Content-MD5']  : '';
                        cannonicalHeaders += '\n';
                        cannonicalHeaders += config['headers'].hasOwnProperty('Content-Type') ? config['headers']['Content-Type'] : '';
                        cannonicalHeaders += '\n';
                        if (config['headers'].hasOwnProperty('x-dlabs-date'))  {
                            cannonicalHeaders += config['headers']['x-dlabs-date'];
                        }
                        else {
                            cannonicalHeaders += config['headers'].hasOwnProperty('Date') ? config['headers']['Date'] : '';
                        }
                        cannonicalHeaders += '\n';
                    }
                    return cannonicalHeaders;
                },
                getDlabHeaders: function(config) {
                    var dlabHeaders = '';
                    if (config.hasOwnProperty('headers')) {
                        var headersLen = 0,
                            keys;

                        // - dlabHeaders are the name value pairs, separated by a ":", 
                        // in lexical order by name, of headers begining with  "x-dlabs", 
                        // excluding "x-dlabs-date"
                        keys = Object.keys(config['headers']).sort();
                        headersLen = keys.length;
                        dlabHeaders = '';
                        for (var i = 0; i < headersLen; i++)
                        {
                            if (keys[i].startsWith('x-dlabs', 0) && keys[i] !== 'x-dlabs-date') {
                                dlabHeaders += keys[i];
                                dlabHeaders += ':';
                                dlabHeaders += config['headers'][keys[i]];
                                dlabHeaders += '\n';
                            }
                        }
                    }
                    return dlabHeaders;
                },
                getUri: function (config){
                    var uri = '';

                    uri += config.hasOwnProperty('url')  ? config['url']  : '';
                    uri += '\n';
                    return uri;
                },
                getQueryParameters: function(config) {
                    var queryParameters = '';
                    if (config.hasOwnProperty('params')) {
                        var len,
                            keys;

                        // queryParameters are the name value pairs, separated by 
                        // a ":", in lexical order by name, of the query parameters
                        keys = Object.keys(config['params']).sort();
                        len = keys.length;
                        for (var i = 0; i < len; i++)
                        {
                            queryParameters += keys[i];
                            queryParameters += ':';
                            queryParameters += config['params'][keys[i]];
                            queryParameters += '\n';
                        }
                    }
                    return queryParameters;
                },
                buildStringToSign: function(config) {
                    var stringToSign ='';

                    stringToSign += service.getHttpVerb(config);
                    stringToSign += service.getCannonicalHeaders(config);
                    stringToSign += service.getDlabHeaders(config);
                    stringToSign += service.getUri(config);
                    stringToSign += service.getQueryParameters(config);

                    return stringToSign;
                },
                buildStringToSignParameters: function(httpVerb,cannonicalHeaders, dlabHeaders, uri, queryParameters) {
                    var stringToSign ='';

                    stringToSign += httpVerb;
                    stringToSign += cannonicalHeaders;
                    stringToSign += dlabHeaders;
                    stringToSign += uri;
                    stringToSign += queryParameters;

                    return stringToSign;
                }
        };
        return service;
    }
)
.factory('SignRequestInterceptor', ['$rootScope', '$q', '$location', 'StringToSign', 'SecurityToken', 'DLabsDate',
    function($rootScope, $q, $location, StringToSign, SecurityToken, DLabsDate) {
        var signRequestInterceptor = {
            request: function(config) {
                var stringToSign,
                    requestSignature,
                    authenticationKey,
                    hash;

                // Only sign REST requests that config
                // - is defined
                // - is an object
                // - has a url property
                // - the url property starts with resource
                // - the url is not a resource/token request
                if (typeof config === null || typeof config !== 'object') {return config;}
                if (!config.hasOwnProperty('url')) {return config;}
                if (config['url'].indexOf('resources') !== 0) {return config;}
                if (config['url'].indexOf('resources/token') === 0) {return config;}

                // Add the x-dlabs-date custom header. This is being done to ensure
                // that the date is formatted as specified in DLABS-84
                if (!config.hasOwnProperty('headers')) {
                    config['headers'] = {};
                }
                config['headers']['x-dlabs-date'] = DLabsDate.toUTC(new Date());
                //console.log('DateUTC: ' + config['headers']['x-dlabs-date']);

                // Get the string to sign
                stringToSign = StringToSign.buildStringToSignParameters(
                    StringToSign.getHttpVerb(config),
                    StringToSign.getCannonicalHeaders(config),
                    StringToSign.getDlabHeaders(config),
                    '/ngclient/' + StringToSign.getUri(config),
                    StringToSign.getQueryParameters(config));

                // Compute the request signature
                authenticationKey = SecurityToken.getSignature();
                hash = CryptoJS.HmacSHA1(stringToSign, authenticationKey);
                requestSignature = CryptoJS.enc.Base64.stringify(hash);
                //console.log('Authentication Key: ' + authenticationKey);
                //console.log('String to sign on: ' + stringToSign);
                //console.log('requestSignature: ' + requestSignature);

                // Insert the Authorization request
                config['headers']['Authorization'] = '';
                config['headers']['Authorization'] += 'DLabsT ';
                config['headers']['Authorization'] += SecurityToken.getRealm();
                config['headers']['Authorization'] += ':';
                config['headers']['Authorization'] += SecurityToken.getToken();
                config['headers']['Authorization'] += ':';
                config['headers']['Authorization'] += requestSignature;
                //console.log("config['headers']['Authorization']: " + config['headers']['Authorization']);

                return config;
            },
            response: function(config) {
                return config;
            },
            responseError: function(rejection) {
                if (rejection.status === 401) {
                    // Return a new promise. This is a mechanism to offer the user
                    // the opportunity to login in and have the failed request
                    // to be resubmitted. This is not supported now
                    /*
                    return userService.authenticate().then(function() {
                        return $injector.get('$http')(rejection.config);
                    });
                    */

                    // For now we will issue an dl-unauthorized-event and preserve
                    // the login page
                    //$location.path('/login');
                    $rootScope.$broadcast('dl-unauthorized-event');
                }

                /* If not a 401, do nothing with this error.
                 * This is necessary to make a `responseError`
                 * interceptor a no-op. */
                return $q.reject(rejection);
            }
        };
        return signRequestInterceptor;
    }
])
.factory('DLabsDate',
    function() {
        return {
            toUTC: function(date) {
                var utcDate = '',
                    aux;

                if (Object.prototype.toString.call(date) !== '[object Date]') {return null;}

                utcDate += date.getUTCFullYear();
                utcDate += '-';
                aux = date.getUTCMonth() + 1;
                utcDate += aux < 10 ? '0'+ aux : aux;
                utcDate += '-';
                utcDate += date.getUTCDate()  < 10 ? '0'+ date.getUTCDate() : date.getUTCDate();
                utcDate += 'T';
                utcDate += date.getUTCHours() < 10 ? '0'+ date.getUTCHours() : date.getUTCHours();
                utcDate += ':';
                utcDate += date.getUTCMinutes() < 10 ? '0'+ date.getUTCMinutes() : date.getUTCMinutes();
                utcDate += ':';
                utcDate += date.getUTCSeconds() < 10 ? '0'+ date.getUTCSeconds() : date.getUTCSeconds();
                utcDate += '.';
                if (date.getUTCMilliseconds() < 10) {
                    utcDate += '00' + date.getUTCMilliseconds();
                }
                else {
                    if (date.getUTCMilliseconds() < 100) {
                        utcDate += '0' + date.getUTCMilliseconds();
                    }
                    else {
                        utcDate += date.getUTCMilliseconds();
                    }
                }
                utcDate += 'Z';

                return utcDate;
            }
        };
    }
)
.config(['$httpProvider',
    function($httpProvider) {
        $httpProvider.interceptors.push('SignRequestInterceptor');
    }
]);
