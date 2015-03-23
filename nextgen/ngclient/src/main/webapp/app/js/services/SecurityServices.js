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
        var httpVerb,
            cannonicalHeaders,
            dlabHeaders,
            uri,
            queryParameters,
            service = {
    			setHttpVerb: function (config) {
                   	httpVerb = config.hasOwnProperty('method')  ? config['method']+'\n'  : ''+'\n';
                },
                getHttpVerb: function () {
                	return httpVerb;
                },
                setCannonicalHeaders: function(config) {
     				// Canonical Headers are: Content-MD5, Content-Type, Date
     				// The value is "" if the header is not present.
     				// If the header "x-dlabs-date" is present, it's value is output for "Date"
    				cannonicalHeaders = '';
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
                },
                getCannonicalHeaders: function (config) {
                    //service.setCannonicalHeaders(config);
                    return cannonicalHeaders;
                },
                setDlabHeaders: function(config) {
     	       		dlabHeaders = '';
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
                },
                getDlabHeaders: function(config) {
                    //service.setDlabHeaders(config);
                    return dlabHeaders;
                },
                setUri: function(config) {
                    uri = '';
     	           	uri += config.hasOwnProperty('url')  ? config['url']  : '';
                    uri += '\n';
                },
                getUri: function (){
                	return uri;
                },
                setQueryParameters: function(config) {
                    queryParameters = '';
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
                },
                getQueryParameters: function() {
                	return queryParameters;
                },
                buildStringToSign: function(config) {
                    var stringTosign ='';

                    service.setHttpVerb(config);
                    stringTosign += service.getHttpVerb(config);
                    stringTosign += '\n';

                    service.setCannonicalHeaders(config);
                    stringTosign += service.getCannonicalHeaders(config);
                    stringTosign += '\n';

                    service.setDlabHeaders(config);
                    stringTosign += service.getDlabHeaders(config);
                    stringTosign += '\n';

                    service.setUri(config);
                    stringTosign += service.getUri(config);
                    stringTosign += '\n';

                    service.setQueryParameters(config);
                    stringTosign += service.getQueryParameters(config);
                    stringTosign += '\n';

                	return stringTosign;
                }
            };
        return service;
    }
);
