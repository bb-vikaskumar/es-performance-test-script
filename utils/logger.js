'use strict'

const bunyan = require('bunyan');

const SVC_NAME = 'es-script';
const LOG_LEVEL = 'debug';


class Logger {

    static init() {
        this.log = bunyan.createLogger({
            name: SVC_NAME,
            serializers: {
                error: bunyan.stdSerializers.err
            },
            streams: [
                {
                    level: LOG_LEVEL,
                    stream: process.stdout,
                    // path: 'logs/process.log'
                }
            ]
        })
    }

    static getLog(args) {
        return this.log.child(args);
    }
}

if(!Logger.log) {
    Logger.init();
}

module.exports = Logger;