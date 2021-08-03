const MonacoWebpackPlugin = require('monaco-editor-webpack-plugin');
const path = require('path');

module.exports = function override(config, env) {
    config.plugins.push(new MonacoWebpackPlugin({
        languages: ['sql']
    }));
    return config;
}
