/* Exports a function which returns an object that overrides the default &
 *   plugin file patterns (used widely through the app configuration)
 *
 * To see the default definitions for Lineman's file paths and globs, see:
 *
 *   - https://github.com/linemanjs/lineman/blob/master/config/files.coffee
 */
module.exports = function(lineman) {
  //Override file patterns here
  return {
    js: {
      vendor: [
        "vendor/js/hmac-md5.js",
        "vendor/js/hmac-sha1.js",
        "vendor/js/hmac-core-min.js",
        "vendor/js/hmac-enc-utf16-min.js",
        "vendor/js/hmac-enc-base64-min.js",
        "vendor/js/jQuery.js",
        "vendor/js/bootstrap.js",
        "vendor/js/angular.js",
        "vendor/js/**/*.js"
      ],
      app: [
        "app/js/app.js",
        "app/js/**/*.js"
      ]
    },

    less: {
      compile: {
        options: {
          paths: [
            "vendor/css/**/*.css",
            "app/css/**/*.less"
          ]
        }
      }
    }
  };
};
