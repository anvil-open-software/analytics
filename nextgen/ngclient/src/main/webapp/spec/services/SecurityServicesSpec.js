xdescribe('Unit: Testing SecurityServices Module', function() {
    beforeEach(function() {
        module("app");
    });

    describe('SecurityToken Service', function() {
        var loginData = {"signatureKey":"8921cd65-41dc-4289-89b8-5b2543e4590e","token":"superuser","realm":"Dematic"};

        it('should be defined',
            inject(['SecurityToken', function(SecurityToken) {
                expect(SecurityToken).not.toBe(null);
            }])
        );

        it('should have a complete set of  methods',
            inject(['SecurityToken', function(SecurityToken) {
                expect(SecurityToken.getSignature).not.toBe(null);
                expect(SecurityToken.getToken).not.toBe(null);
                expect(SecurityToken.getRealm).not.toBe(null);
            }])
        );

        it('should set and get the token object',
            inject(['SecurityToken', function(SecurityToken) {
                SecurityToken.set(loginData);

                expect(SecurityToken.getSignature()).toBe(loginData.signatureKey);
                expect(SecurityToken.getToken()).toBe(loginData.token);
                expect(SecurityToken.getRealm()).toBe(loginData.realm);
            }])
        );
    })

    describe('StringToSign Service', function() {
        var config = {
            method: 'GET',
            url: 'http://127.0.0.1:8080/admin/resources/token',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': 'DLabsU Dematic:superuser:abcd1234',
                'Date': '2015-02-19T16:03:29.407Z'
            }
        };

        it('should be defined',
            inject(['StringToSign', function(StringToSign) {
                expect(StringToSign).not.toBe(null);
            }])
        );
        it('should have a complete set of methods',
            inject(['StringToSign', function(StringToSign) {
                expect(StringToSign.setHttpVerb).not.toBe(null);
                expect(StringToSign.getHttpVerb).not.toBe(null);

                expect(StringToSign.setCannonicalHeaders).not.toBe(null);
                expect(StringToSign.getCannonicalHeaders).not.toBe(null);

                expect(StringToSign.setDlabHeaders).not.toBe(null);
                expect(StringToSign.getDlabHeaders).not.toBe(null);

                expect(StringToSign.setUri).not.toBe(null);
                expect(StringToSign.getUri).not.toBe(null);

                expect(StringToSign.setQueryParameters).not.toBe(null);
                expect(StringToSign.getQueryParameters).not.toBe(null);

                expect(StringToSign.buildStringToSign).not.toBe(null);
                expect(StringToSign.buildStringToSignParameters).not.toBe(null);
            }])
        );
        it('should set up the httpverb',
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                expected += config['method'];
                expected += '\n';

                //StringToSign.setHttpVerb(config);
                expect(expected).toBe(StringToSign.getHttpVerb(config));
            }])
        );
        it('should set up the CannonicalHeaders - without x-dlabs-date',
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                // do not have Content-Md5
                expected += '\n';
                expected += config['headers']['Content-Type'];
                expected += '\n';
                expected += config['headers']['Date'];
                expected += '\n';

                expect(expected).toBe(StringToSign.getCannonicalHeaders(config));
            }])
        );
        it('should set up the CannonicalHeaders - with x-dlabs-date',
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                config['headers']['x-dlabs-date'] = '2015-02-20T01:03:29.407Z';

                // do not have Content-Md5
                expected += '\n';
                expected += config['headers']['Content-Type'];
                expected += '\n';
                expected += config['headers']['x-dlabs-date'];
                expected += '\n';

                expect(expected).toBe(StringToSign.getCannonicalHeaders(config));
            }])
        );
        it('should set up the DLabsHeaders without x-lab-date',
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                config['headers']['x-dlabs-value3'] = 'CCCCC';
                config['headers']['x-dlabs-value1'] = 'AAAAA';
                config['headers']['x-dlabs-value2'] = 'BBBBB';

                expected += 'x-dlabs-value1';
                expected += ':';
                expected += config['headers']['x-dlabs-value1'];
                expected += '\n';
                expected += 'x-dlabs-value2';
                expected += ':';
                expected += config['headers']['x-dlabs-value2'];
                expected += '\n';
                expected += 'x-dlabs-value3';
                expected += ':';
                expected += config['headers']['x-dlabs-value3'];
                expected += '\n';

                expect(expected).toBe(StringToSign.getDlabHeaders(config));
            }])
        );
        it('should set up the DLabsHeaders with x-lab-date',
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                config['headers']['x-dlabs-value3'] = 'CCCCC';
                config['headers']['x-dlabs-date'] = '2015-02-20T01:03:29.407Z';
                config['headers']['x-dlabs-value1'] = 'AAAAA';
                config['headers']['x-dlabs-value2'] = 'BBBBB';

                expected += 'x-dlabs-value1';
                expected += ':';
                expected += config['headers']['x-dlabs-value1'];
                expected += '\n';
                expected += 'x-dlabs-value2';
                expected += ':';
                expected += config['headers']['x-dlabs-value2'];
                expected += '\n';
                expected += 'x-dlabs-value3';
                expected += ':';
                expected += config['headers']['x-dlabs-value3'];
                expected += '\n';

                expect(expected).toBe(StringToSign.getDlabHeaders(config));
            }])
        );
        it('should set up the uri',
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                expected += config['url'];
                expected += '\n';

                expect(expected).toBe(StringToSign.getUri(config));
            }])
        );
        it('should set up the query parameters', function() {
            inject(['StringToSign', function(StringToSign) {
                var expected = '';

                config['params'] = {};
                config['params']['param3'] = 'CCCCC';
                config['params']['param1'] = 'AAAAA';
                config['params']['param2'] = 'BBBBB';

                expected += 'param1'
                expected += ':';
                expected += config['params']['param1'];
                expected += '\n';
                expected += 'param2'
                expected += ':';
                expected += config['params']['param2'];
                expected += '\n';
                expected += 'param3'
                expected += ':';
                expected += config['params']['param3'];
                expected += '\n';

                expect(expected).toBe(StringToSign.getQueryParameters(config));
            }])
        });
        it('should set up the string to sign using config', function() {
            inject(['StringToSign', function(StringToSign){
                var expected = '',
                    stringToSign;

                config['headers']['x-dlabs-value3'] = 'CCCCC';
                config['headers']['x-dlabs-date'] = '2015-02-20T01:03:29.407Z';
                config['headers']['x-dlabs-value1'] = 'AAAAA';
                config['headers']['x-dlabs-value2'] = 'BBBBB';
                config['params'] = {};
                config['params']['param3'] = 'CCCCC';
                config['params']['param1'] = 'AAAAA';
                config['params']['param2'] = 'BBBBB';

                expected += StringToSign.getHttpVerb(config);
                expected += StringToSign.getCannonicalHeaders(config);
                expected += StringToSign.getDlabHeaders(config);
                expected += StringToSign.getUri(config);
                expected += StringToSign.getQueryParameters(config);
                expect(expected).toBe(StringToSign.buildStringToSign(config));
            }])
        });
        it('should set up the string to sign using parameters instead of config', function() {
            inject(['StringToSign', function(StringToSign){
                var expected = '',
                    stringToSign;

                config['headers']['x-dlabs-value3'] = 'CCCCC';
                config['headers']['x-dlabs-date'] = '2015-02-20T01:03:29.407Z';
                config['headers']['x-dlabs-value1'] = 'AAAAA';
                config['headers']['x-dlabs-value2'] = 'BBBBB';
                config['params'] = {};
                config['params']['param3'] = 'CCCCC';
                config['params']['param1'] = 'AAAAA';
                config['params']['param2'] = 'BBBBB';

                expected += StringToSign.getHttpVerb(config);
                expected += StringToSign.getCannonicalHeaders(config);
                expected += StringToSign.getDlabHeaders(config);
                expected += StringToSign.getUri(config);
                expected += StringToSign.getQueryParameters(config);

                expect(expected).toBe(StringToSign.buildStringToSignParameters(
                        StringToSign.getHttpVerb(config),
                        StringToSign.getCannonicalHeaders(config),
                        StringToSign.getDlabHeaders(config),
                        StringToSign.getUri(config),
                        StringToSign.getQueryParameters(config)
                    )
                );
            }])
        });
    })
    describe('DLabsDate Service', function() {
        it('should be defined',
            inject(['DLabsDate', function(DLabsDate) {
                expect(DLabsDate).not.toBe(null);
            }])
        );
        it('should have all methods',
            inject(['DLabsDate', function(DLabsDate) {
                expect(DLabsDate.toUTC).not.toBe(null);
            }])
        );
        it('should fail when handling a non-date object',
            inject (['DLabsDate', function (DLabsDate) {
                expect(DLabsDate.toUTC(123)).toBeNull();
                expect(DLabsDate.toUTC('abcd')).toBeNull();
                expect(DLabsDate.toUTC({'a': 1, 'b': 2})).toBeNull();
            }])
        );
        it('should convert 2015-1-1T1:0:3.333 correctly',
            inject(['DLabsDate', function(DLabsDate) {
                var localDate = new Date(2015, 1, 1, 1, 0, 3, 333),
                    utcDate = '',
                    aux;

                //convert it to the desired UTC method
                utcDate += localDate.getUTCFullYear();
                utcDate += '-';
                aux = localDate.getUTCMonth() + 1;
                utcDate += aux < 10 ? '0'+ aux : aux;
                utcDate += '-';
                utcDate += localDate.getUTCDate()  < 10 ? '0'+ localDate.getUTCDate() : localDate.getUTCDate();
                utcDate += 'T';
                utcDate += localDate.getUTCHours() < 10 ? '0'+ localDate.getUTCHours() : localDate.getUTCHours();
                utcDate += ':';
                utcDate += localDate.getUTCMinutes() < 10 ? '0'+ localDate.getUTCMinutes() : localDate.getUTCMinutes();
                utcDate += ':';
                utcDate += localDate.getUTCSeconds() < 10 ? '0'+ localDate.getUTCSeconds() : localDate.getUTCSeconds();
                utcDate += '.';
                if (localDate.getUTCMilliseconds() < 10) {
                    utcDate += '00' + localDate.getUTCMilliseconds();
                }
                else {
                    if (localDate.getUTCMilliseconds() < 100) {
                        utcDate += '0' + localDate.getUTCMilliseconds();
                    }
                    else {
                        utcDate += localDate.getUTCMilliseconds();
                    }
                }
                utcDate += 'Z';

                expect(DLabsDate.toUTC(localDate)).toBe(utcDate);
            }])
        );
    })
});

