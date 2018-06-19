/*
用于远程执行Shell脚本全量更新neo4j的数据
wrote by tzf, 2017/12/27
*/
const async = require('async');
const SSH2Utils = require('ssh2-utils');
const ssh = new SSH2Utils();
const path = require('path');
const log4js = require('log4js');
const moment = require('moment');
const req = require('require-yml');
const config = req('./config/source.yml');

/*For test
let shellFileName = 'importTotalData.sh';
let localShellPath = path.join(process.cwd(),'\\shell\\');
console.log('localShellPath: ' +localShellPath);
let remoteShellPath = '/home/neo4j/';

let logsFileName = 'importTotalData.log';
let localLogsPath = path.join(process.cwd(),'\\logs\\');
console.log('localLogsPath: ' +localLogsPath);
let remoteLogsPath = '/home/neo4j/logs/';
let putFileRes = putFiles( ip, shellFileName, localShellPath, remoteShellPath, callback );
console.log('putFileResult: ' +putFileRes);

let cmdShellRes = cmdShell( cmd, ip, callback );
console.log('cmdShellResult: '+cmdShellRes);

let getFilesRes = getFiles( ip, logsFileName, localLogsPath, remoteLogsPath, callback );
console.log('getFilesResult: ' +getFilesRes);
*/

log4js.configure({
    appenders: {
        'out': {
            type: 'file',         //文件输出
            filename: 'logs/updateData.log',
            maxLogSize: config.logInfo.maxLogSize
        }
    },
    categories: { default: { appenders: ['out'], level: 'info' } }
});

const logger = log4js.getLogger();

//过滤字符串中的特殊字符
function stripscript(s) {
    let pattern = new RegExp("[`~!@#$^&*()=|{}':;',\\[\\].<>/?~！@#￥……&*（）&mdash;—|{}【】‘；：”“'。，、？]");
    let rs = "";
    for (let i = 0; i < s.length; i++) {
        rs = rs + s.substr(i, 1).replace(pattern, '');
    }
    return rs;
}

let executeShell = {
    //exec linux shell on remote-servers
    startCmdShell: async function (cmd, ser) {
        return new Promise(async (resolve, reject) => {
            let cmdResultMsg = `neo4j server_${ser.id} Execute the shell successfully!`;
            if (!cmd || !ser.ip || !ser.ip.length) {
                console.error("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, neo4j server_${ser.id} cmdShell ERR: 缺少参数 `);
                logger.error(`neo4j server_${ser.id} cmdShell ERR: 缺少参数! `);
                return reject({});
            }
            else {
                var results = [];
                async.waterfall([
                    function (cb1) {
                        var servers = [];
                        for (var i = 0; i < ser.ip.length; i++) {
                            var _server = {};
                            _server['host'] = ser.ip[i];
                            _server['port'] = ser.port;
                            _server['username'] = ser.username;
                            _server['password'] = ser.password;
                            _server['readyTimeout'] = 99999;
                            servers.push(_server);
                        }
                        cb1(null, servers);
                    },
                    function (servers, cb1) {
                        async.each(servers, function (server, cb2) {
                            var _result = {};
                            ssh.exec(server, cmd, function (err, stdout, stderr) {
                                if (stderr) {
                                    console.error(`neo4j server_${ser.id} stderr: ` + stderr);
                                    logger.error(`neo4j server_${ser.id} stderr: ` + stderr);
                                }
                                _result['ip'] = server.host;
                                _result['cmdResult'] = stdout.replace('\n\n', '').replace('\n', '');
                                results.push(_result);
                                // conn.end();
                                cb2();
                            })
                        }, function (err) {
                            cb1(err, results);
                        })
                    }
                ], function (err, result) {
                    if (err) {
                        // throw err;
                        console.error(`neo4j server_${ser.id} err: ` + err);
                        logger.error(`neo4j server_${ser.id} err: ` + err);
                        return reject(err);
                    }
                    console.log(`neo4j server_${ser.id} cmdResultMsg: ` + cmdResultMsg + '\n' + 'cmdResultDetail: ' + results[0].cmdResult);
                    logger.info(`neo4j server_${ser.id} cmdResultMsg: ` + cmdResultMsg + '\n' + 'cmdResultDetail: ' + results[0].cmdResult);
                    return resolve({ 'ok': 1, 'cmdResultMsg': cmdResultMsg, 'cmdResultDetail': results[0].cmdResult });
                })
            }
        });

    },

    //远程执行cmd
    startExecuteCmd: async function (server, cmd) {
        return new Promise(async (resolve, reject) => {
            ssh.exec(server, cmd, function (err, stdout, stderr) {
                if (err) {
                    console.error(err);
                    logger.error(err);
                    return reject(err);
                }
                console.log(`cmd: '${cmd}' execute result: ` + stdout);
                let cmdResult = stripscript(stdout);
                cmdResult = cmdResult.replace(/\u0000|\u0001|\u0002|\u0003|\u0004|\u0005|\u0006|\u0007|\u0008|\u0009|\u000a|\u000b|\u000c|\u000d|\u000e|\u000f|\u0010|\u0011|\u0012|\u0013|\u0014|\u0015|\u0016|\u0017|\u0018|\u0019|\u001a|\u001b|\u001c|\u001d|\u001e|\u001f|\d+/g, "");
                if (cmdResult) {
                    return resolve(cmdResult);
                }
                else {
                    return resolve({});
                }

            });
        });

    },

    //exec linux shell on remote-servers
    startPutFiles: async function (ser, filename, localPath, remotePath) {
        return new Promise(async (resolve, reject) => {
            let putFilesMsg = `Put the file: ${filename} to remote server_${ser.id} successfully!`;
            if (!ser.ip || !filename || !remotePath || !localPath) {
                console.error("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, neo4j server_${ser.id} cmdShell ERR: 缺少参数 `);
                logger.error(`neo4j server_${ser.id} cmdShell ERR: 缺少参数! `);
                return reject({});
            }
            else {
                async.waterfall([
                    function (cb1) {
                        var servers = [];
                        for (var i = 0; i < ser.ip.length; i++) {
                            var _server = {};
                            _server['host'] = ser.ip[i];
                            _server['port'] = ser.port;
                            _server['username'] = ser.username;
                            _server['password'] = ser.password;
                            _server['readyTimeout'] = 99999;
                            servers.push(_server);
                        }
                        cb1(null, servers);
                    },
                    function (servers, cb1) {
                        async.each(servers, function (server, cb2) {
                            var _localFile = localPath + filename;
                            var _remoteFile = remotePath + filename;
                            ssh.putFile(server, _localFile, _remoteFile, function (err) {
                                if (err) {
                                    console.error(`neo4j server_${ser.id} stderr: ` + err);
                                    logger.error(`neo4j server_${ser.id} stderr: ` + err);
                                }
                                // conn.end();
                                cb2();
                            })
                        }, function (err) {
                            cb1();
                        })
                    }
                ], function (err) {
                    if (err) {
                        // throw err;
                        console.error(`neo4j server_${ser.id} err: ` + err);
                        logger.error(`neo4j server_${ser.id} err: ` + err);
                        return reject(err);
                    }
                    return resolve({ 'ok': 1, 'putFilesMsg': putFilesMsg });
                })
            }
        });
    },

    //get file from remote-servers function
    startGetFiles: async function (ips, filename, remotePath, localPath, callback) {
        let getFilesMsg = 'Get the logs file from remote server successfully!';
        if (!ips || !filename || !remotePath || !localPath) {
            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ",getFiles ERR: 缺少参数 ");
            logger.info("getFiles ERR: 缺少参数! ");
        }
        else {
            async.waterfall([
                function (cb1) {
                    var servers = [];
                    for (var i = 0; i < ips.length; i++) {
                        var _server = {};
                        _server['host'] = ips[i];
                        _server['port'] = `${config.sshServer.port}`;
                        _server['username'] = `${config.sshServer.username}`;
                        _server['password'] = `${config.sshServer.password}`;
                        servers.push(_server)
                    }
                    cb1(null, servers)
                },
                function (servers, cb1) {
                    async.each(servers, function (server, cb2) {
                        async.series([
                            function (cb3) {
                                var localServer = { host: 'localhost', username: '', password: '' };
                                var _localPath = localPath + server.host;
                                ssh.mkdir(localServer, _localPath, function (err, server, conn) {
                                    if (err) {
                                        console.log(err);
                                        logger.info(err);
                                    }
                                    conn.end();
                                    cb3(null, 'one')
                                })
                            },
                            function (cb3) {
                                var _remoteFile = remotePath + filename;
                                var _localFile = localPath + server.host + '/' + filename;
                                ssh.getFile(server, _remoteFile, _localFile, function (err, server, conn) {
                                    if (err) {
                                        console.log(err);
                                        logger.info(err);
                                    }
                                    // conn.end();
                                    cb3(null, 'two')
                                })
                            }
                        ], function (err, c) {
                            cb2()
                        })
                    }, function (err) {
                        cb1()
                    })
                }
            ], function (err) {
                if (typeof callback == "function") {
                    callback(getFilesMsg);
                }
            })
        }
    }

}


module.exports = executeShell;