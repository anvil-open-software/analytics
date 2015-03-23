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
                        var len = 0,
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
                    stringToSign += '\n';

                    stringToSign += service.getCannonicalHeaders(config);
                    stringToSign += '\n';

                    stringToSign += service.getDlabHeaders(config);
                    stringToSign += '\n';

                    stringToSign += service.getUri(config);
                    stringToSign += '\n';

                    stringToSign += service.getQueryParameters(config);
                    stringToSign += '\n';

                    return stringToSign;
                }
            };
        return service;
    }
);
