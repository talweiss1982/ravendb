var path = require('path');

var STUDIO_TEST_DIR = '../../test/Studio';

var paths = {
    handlersToParse: [
        '../Raven.Server/**/*Handler.cs'
    ],
    configurationFilesToParse:[
        '../Raven.Server/Config/Categories/**/*Configuration.cs'
    ],
    configurationConstants: '../Raven.Client/Constants.cs',
    constantsTargetDir: './typescript/',
    typingsConfig: './typings.json',
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/',

    aceDir: './wwwroot/Content/ace/',

    test: {
        dir: STUDIO_TEST_DIR,
        tsSource: path.join(STUDIO_TEST_DIR, 'typescript/**/*.ts'),
        tsOutput: path.join(STUDIO_TEST_DIR, 'js'),
        setup: path.join(STUDIO_TEST_DIR, 'setup'),
        html: path.join(STUDIO_TEST_DIR, 'test.html')
    },

    lessSource: [
        './wwwroot/Content/css/styles.less',
        './wwwroot/Content/css/legacy_styles.less'],
    lessSourcesToWatch: [
        './wwwroot/Content/css/**/*.less',
    ],
    lessTarget: './wwwroot/Content/',
    lessTargetSelector: './wwwroot/Content/**/*.css',

    releaseTarget: './build/',
    bowerSource: './wwwroot/lib/',
    cssToMerge: [
        'wwwroot/lib/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.css',
        'wwwroot/lib/bootstrap-select/dist/css/bootstrap-select.css',
        'wwwroot/lib/bootstrap-multiselect/dist/css/bootstrap-multiselect.css',
        'wwwroot/lib/Durandal/css/durandal.css',
        'wwwroot/lib/animate.css/animate.css',
        'wwwroot/lib/toastr/toastr.css',
        'wwwroot/lib/prism/themes/prism-dark.css',
        'wwwroot/Content/css/styles.css',
        'wwwroot/Content/css/legacy_styles.css'
    ],
    externalLibs: [
        "jquery/dist/jquery.js",
        'lodash/dist/lodash.js',
        'prism/prism.js',
        'prism/components/prism-javascript.js',
        "blockUI/jquery.blockUI.js",
        "knockout/dist/knockout.debug.js",
        "knockout-validation/dist/knockout.validation.js",
        "knockout.dirtyFlag/index.js",
        "knockout-delegatedEvents/build/knockout-delegatedEvents.js",
        "knockout-postbox/build/knockout-postbox.js",
        "moment/moment.js",
        "bootstrap/dist/js/bootstrap.js",
        "eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min.js",
        "bootstrap-contextmenu/bootstrap-contextmenu.js",
        "bootstrap-multiselect/dist/js/bootstrap-multiselect.js",
        "bootstrap-select/dist/js/bootstrap-select.js",
        "jwerty/jwerty.js",
        "jquery-fullscreen/jquery.fullscreen.js",
        "spin.js/spin.js",
        "google-analytics/index.js"
    ]
};

paths.releaseTargetApp = path.join(paths.releaseTarget, 'App');
paths.releaseTargetContent = path.join(paths.releaseTarget, 'Content');
paths.releaseTargetContentCss = path.join(paths.releaseTargetContent, 'css');

module.exports = paths;
