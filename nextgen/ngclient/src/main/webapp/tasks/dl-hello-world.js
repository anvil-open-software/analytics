module.exports = function(grunt) {
    return grunt.registerTask('dlHelloWorld', 'DLABS Hello World', function(){
        grunt.log.writeln("Hello from DLABS");
    });
};
