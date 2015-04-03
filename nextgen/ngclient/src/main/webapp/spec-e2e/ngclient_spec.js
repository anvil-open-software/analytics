describe('ngclient', function() {
    describe("index", function () {
        it("should display the correct title", function () {
            browser.get('http://localhost:8080/ngclient/');
            expect(browser.getTitle()).toBe('ngclient');
        });
    });
});
