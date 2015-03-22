module.exports = function(grunt) {
    return grunt.registerTask('dl-dev-copy', 'Copy UI artifacts to wildfly deployment folder', ['copy:dl-dev-copy']);
};