angular.module("app").run(["$templateCache", function($templateCache) {

  $templateCache.put("books_http.html",
    "<!-- With $http\n" +
    "\n" +
    "Note the extra 'books.data', which is needed because $resource does fancy promise resolution\n" +
    "when attached to a controller scope and $http does not -->\n" +
    "\n" +
    "<ul class=\"books\">\n" +
    "  <li class=\"panel\" ng-repeat=\"book in books.data\">\n" +
    "    {{ book.title }} by {{ book.author }}\n" +
    "  </li>\n" +
    "</ul>\n" +
    "\n"
  );

  $templateCache.put("books_resource.html",
    "<!-- With $resource -->\n" +
    "\n" +
    "<ul class=\"books\">\n" +
    "  <li class=\"panel\" ng-repeat=\"book in books\">\n" +
    "    {{ book.title }} by {{ book.author }}\n" +
    "  </li>\n" +
    "</ul>\n"
  );

  $templateCache.put("home.html",
    "<div id=\"home\" class=\"row\">\n" +
    "  <div class=\"large-6 large-offset-3\">\n" +
    "    <h2>Welcome to the {{ title }} page!</h2>\n" +
    "\n" +
    "    <div class=\"alert-box\">{{ message }}</div>\n" +
    "\n" +
    "    <ul class=\"small-block-grid-2\">\n" +
    "      <li>\n" +
    "        <img class=\"th\" src=\"img/demo1.jpg\" shows-message-when-hovered message=\"Im the first house.\">\n" +
    "      </li>\n" +
    "      <li>\n" +
    "        <img class=\"th\" src=\"img/demo2.jpg\" shows-message-when-hovered message=\"Im the second house.\">\n" +
    "      </li>\n" +
    "    </ul>\n" +
    "\n" +
    "    <div class=\"row\">\n" +
    "      <div class=\"large-12 columns\">\n" +
    "        <button ng-click=\"logout()\" class=\"button large expand radius\">Log Out</button>\n" +
    "      </div>\n" +
    "    </div>\n" +
    "  </div>\n" +
    "</div>\n"
  );

  $templateCache.put("login.html",
    "<div id=\"login\" class=\"row\">\n" +
    "  <div class=\"large-6 large-offset-3\">\n" +
    "    <form ng-submit=\"login()\">\n" +
    "      <fieldset class=\"radius\">\n" +
    "        <div class=\"row\">\n" +
    "          <div class=\"large-6 columns\">\n" +
    "            <input type=\"text\" name=\"username\" placeholder=\"username\" ng-model=\"credentials.username\" required/>\n" +
    "          </div>\n" +
    "            <div class=\"large-6 columns\">\n" +
    "              <input type=\"password\" name=\"password\" placeholder=\"password\" ng-model=\"credentials.password\" required/>\n" +
    "          </div>\n" +
    "        </div>\n" +
    "\n" +
    "        <div class=\"row\">\n" +
    "          <div class=\"large-12 columns\">\n" +
    "            <button id=\"log-in\" type=\"submit\" class=\"button large expand radius\">Log In</button>\n" +
    "          </div>\n" +
    "        </div>\n" +
    "      </fieldset>\n" +
    "    </form>\n" +
    "  </div>\n" +
    "</div>\n"
  );

}]);
