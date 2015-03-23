module.exports = function(grunt) {
    return grunt.registerTask('dl-dev-uglifly', 'Uglify js files including maps to wildfly deployment folder', ['uglify:dl-dev-uglifly']);
};