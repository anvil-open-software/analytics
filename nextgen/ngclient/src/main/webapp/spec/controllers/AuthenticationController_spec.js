describe("controller: SigninController ($httpBackend.expect().respond, vanilla jasmine, javascript)", function() {
    var controller,
        rootScope,
        location,
        routeParams,
        authenticationService,
        authenticatedUser,
        securityToken,
        scope;

    beforeEach(function() {
        module("app");
    });

    beforeEach(inject(function($controller, $rootScope, $location, $routeParams, AuthenticationService, AuthenticatedUser, SecurityToken) {
        controller = $controller;
        rootScope = $rootScope;
        location = $location;
        routeParams = $routeParams;
        authenticationService = AuthenticationService;
        authenticatedUser = AuthenticatedUser;
        securityToken = SecurityToken;
        scope = $rootScope.$new();
        this.redirect = spyOn(location, 'path');
    }));

    afterEach(function() {
        //this.$httpBackend.verifyNoOutstandingRequest();
        //this.$httpBackend.verifyNoOutstandingExpectation();
    });

    describe("sets up tenant", function() {
        /*
        it("as null when not in the routeParams", function() {
            this.$httpBackend.expectPOST('/login', this.scope.credentials).respond(200);
            this.scope.login();
            this.$httpBackend.flush();
            expect(this.redirect).toHaveBeenCalledWith('/home');
        });
        */
        it("as null when not in the routeParams", function() {
            var ctrlr = controller ('SigninController', {
                $scope: scope,
                $location: location,
                $routeParams: {},
                AuthenticationService: authenticationService,
                AuthenticatedUser: authenticatedUser,
                SecurityToken: securityToken
            });
            expect(scope.credentials.tenant).toBeNull();
            expect(scope.tenantReadonly).toBeFalsy();
        });
        it("as 'Dematic' when routeParams.tenant is Dematic", function() {
            var ctrlr = controller ('SigninController', {
                $scope: scope,
                $location: location,
                $routeParams: {'tenant': 'Dematic'},
                AuthenticationService: authenticationService,
                AuthenticatedUser: authenticatedUser,
                SecurityToken: securityToken
            });
            expect(scope.credentials.tenant).toBe('Dematic');
            expect(scope.tenantReadonly).toBeTruthy();
        });
    });
});
