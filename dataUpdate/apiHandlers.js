const timingUpdate = require('./timingUpdate.js');
const schedule = require("node-schedule");
const moment = require('moment');
const log4js = require('log4js');
const Promise = require('bluebird');
const executeShell = require('./executeShell.js');
const req = require('require-yml');
const config = req('./config/source.yml');
const path = require('path');
const transactions = require('./transactions.js');
const util = require('util');
const setImmediatePromise = util.promisify(setImmediate);
const sleep = require('system-sleep');
const Redis = require('ioredis');
const redis = new Redis(config.redisUrl[0]);
const redis1 = new Redis(config.redisUrl[1]);
const Redlock = require('redlock');
const lockResource = config.lockInfo.resource[0];
const lockTTL = config.lockInfo.TTL[0];
console.log('lockResource: ' + lockResource + ', lockTTL: ' + lockTTL + ' ms');
let redlock = new Redlock(
    // you should have one client for each independent redis node
    // or cluster
    [redis, redis1],
    {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01, // time in ms

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount: 1000,

        // the time in ms between attempts
        retryDelay: 10000, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter: 200 // time in ms
    }
);

//写入dump文件的信息
const dumpFilePath = path.join(process.cwd(), '/heapDumpFile/');
console.log('dumpFilePath: ' + dumpFilePath);
const heapdump = require('heapdump');
const dumpFileName = `${moment(Date.now()).format("YYYY-MM-DD--HH:mm:ss")}.heapsnapshot`;

const NodeHaproxyStats = require('./nodeHaproxyStats.js');
const HAServerName = config.haproxyInfo.defaultServerName;
const HAServerIP = config.haproxyInfo.defaultServerIP;
const HAServerPort = config.haproxyInfo.defaultServerPort;
console.log('HAServerName: ' + HAServerName + ', HAServerIP: ' + HAServerIP + ', HAServerPort: ' + HAServerPort);
const hs = new NodeHaproxyStats(HAServerIP, HAServerPort);

const remoteServersInfo = [];
const remoteserversNum = config.sshServer.length;
for (let i = 0; i < remoteserversNum; i++) {
    let serverInfo = {};
    serverInfo.host = config.sshServer[i].ips[0];                                             //互为切换的服务器IP信息
    serverInfo.username = config.sshServer[i].username;
    serverInfo.password = config.sshServer[i].password;
    remoteServersInfo.push(serverInfo);
}

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

//定时更新company和relation信息
var rule = new schedule.RecurrenceRule();
rule.dayOfWeek = [0, new schedule.Range(1, 6)];
rule.hour = config.schedule.hour;
rule.minute = config.schedule.minute;
console.log('定时全量更新时间: ' + rule.hour + '时 ' + rule.minute + '分');
logger.info('定时全量更新时间: ' + rule.hour + '时 ' + rule.minute + '分');
schedule.scheduleJob(rule, function () {
    redlock.lock(lockResource, lockTTL).then(async function (lock) {
        let updateFlag = await apiHandlers.timingUpdateTotalData('true');
        if (updateFlag == true || updateFlag == false) {
            // unlock your resource when you are done
            return lock.unlock()
                .catch(function (err) {
                    // we weren't able to reach redis; your lock will eventually
                    // expire, but you probably want to log this error
                    console.error(err);
                    logger.error(err);
                });
        }
    })
});

const waitIndexOnTime = config.timeSetting.waitIndexOn;
console.log('1次等待创建索引时间: ' + waitIndexOnTime + 'ms');
logger.info('1次等待创建索引时间: ' + waitIndexOnTime + 'ms');
const maxWaitIndexOnTimes = config.timeSetting.maxWaitIndexOnTimes;
console.log('最大等待创建索引次数: ' + maxWaitIndexOnTimes + '次');
logger.info('最大等待创建索引次数: ' + maxWaitIndexOnTimes + '次');

const waitGetSysInfoTime = config.timeSetting.waitGetSysInfo;
console.log('1次等待检查neo4j server已成功启动的时间: ' + waitGetSysInfoTime + 'ms');
logger.info('1次等待检查neo4j server已成功启动的时间: ' + waitGetSysInfoTime + 'ms');
const maxWaitSysInfoTimes = config.timeSetting.maxWaitSysInfoTimes;
console.log('最大等待查询sysinfo次数: ' + maxWaitSysInfoTimes + '次');
logger.info('最大等待查询sysinfo次数: ' + maxWaitSysInfoTimes + '次');

let labelName1 = config.indexInfo.nodeLabelName1;
let indexName1 = config.indexInfo.nodeIndexName1;
console.log('neo4j index info: ' + 'labelName1: ' + labelName1 + ', indexName1: ' + indexName1);
logger.info('neo4j index info: ' + 'labelName1: ' + labelName1 + ', indexName1: ' + indexName1);

const errorCode = {
    ARG_ERROR: {
        code: -100,
        msg: "请求参数错误"
    },
    NOTSUPPORT: {
        code: -101,
        msg: "不支持的参数"
    },
    INTERNALERROR: {
        code: -200,
        msg: "服务内部错误"
    },
    NOMATCHINGDATA: {
        code: -505,
        msg: "无匹配数据"
    }
}

function errorResp(err, msg) {
    return { ok: err.code, error: msg || err.msg };
}


//for test, 定时任务获取process的信息
schedule.scheduleJob('1 * * * * *', function () {
    getProcessInfo();
});

function getCpuInfo() {
    const startUsage = process.cpuUsage();
    // { user: 38579, system: 6986 }

    // spin the CPU for 500 milliseconds
    const now = Date.now();
    while (Date.now() - now < 500);

    return (process.cpuUsage(startUsage));
}

//强制执行垃圾回收操作
function forceGC() {
    if (global.gc) {
        global.gc();
        console.log('执行GC操作成功! global.gc: ' + global.gc);
        logger.info('执行GC操作成功! global.gc: ' + global.gc);
    } else {
        console.warn('No GC hook!');
        logger.warn('No GC hook!');
    }
}

//获取进程中的相关信息
function getProcessInfo() {
    let pid = process.pid;
    let memoryUsage = process.memoryUsage();
    let cpuInfo = getCpuInfo();
    let uptime = process.uptime();
    let processInfo = { 'pid': pid, 'cpuInfo': cpuInfo, 'memoryUsage': memoryUsage, 'uptime': `${uptime} s` };
    //如果内存存在不足的风险, 给出警告并执行一次GC操作
    let memoryLeftValue = processInfo.memoryUsage.heapTotal - processInfo.memoryUsage.heapUsed;
    let warnMemoryLeftValue = config.gcInfo.warnMemoryLeftValue;
    if (memoryLeftValue <= warnMemoryLeftValue) {
        heapdump.writeSnapshot(dumpFilePath + dumpFileName, function (err, dumpFilePath) {
            console.log('dump written to', dumpFileName);
        });
        console.warn('memoryLeftValue: ' + memoryLeftValue + 'the heap memory will be exhaust!');
        logger.warn('memoryLeftValue: ' + memoryLeftValue + 'the heap memory will be exhaust!');
        forceGC();
    }
    processInfo.memoryLeft = `${memoryLeftValue / 1048576} mb`;
    console.log('processInfo: ' + JSON.stringify(processInfo));
    logger.info('processInfo: ' + JSON.stringify(processInfo));

}

//远程执行Shell脚本全量导入数据
async function startImportTotalData(ser, shellFileName, remoteShellPath) {
    try {
        // let localShellPath = path.join(process.cwd(), '\\shell\\');       //windows
        let localShellPath = path.join(process.cwd(), '/shell/');            //Ubuntu
        console.log('localShellPath: ' + localShellPath);
        // let cmd = `cd ${remoteShellPath} && chmod +777 ./${shellFileName} && ./${shellFileName}`;
        let cmd = `cd ${remoteShellPath} && chmod +x ./${shellFileName} && ./${shellFileName}`;
        let importDataResult = { cmdShellResult: null, createIndexStatus: null, systemInfo: null };

        let cmdShellRes = await executeShell.startCmdShell(cmd, ser);
        if (cmdShellRes.hasOwnProperty('ok') && cmdShellRes.ok == 1) {
            importDataResult.cmdShellResult = cmdShellRes;
            console.log(JSON.stringify(importDataResult.cmdShellResult));
            logger.info(JSON.stringify(importDataResult.cmdShellResult));
        }
        else {
            importDataResult.cmdShellResult = null;
            console.error('执行startCmdShell失败！');
            logger.error('执行startCmdShell失败！');
        }

        let serId = ser.id;
        let systemResult = null;
        console.log(`startCmdShell_${serId} 执行后sleep ${waitGetSysInfoTime} ms`);
        logger.info(`startCmdShell_${serId} 执行后sleep ${waitGetSysInfoTime} ms`);
        sleep(waitGetSysInfoTime);
        if (cmdShellRes.cmdResultDetail) {                                                                    //判断shellCmd 是否执行成功，即neo4j server 是否导入数据成功
            let retryCount = 0;
            do {
                try {
                    systemResult = await transactions.getSystemInfo(serId);                                 //获取neo4j sysinfo
                    if (systemResult.nodeIdsNum > 0 && systemResult.propertyIdsNum > 0 && systemResult.relationshipIdsNum > 0 && systemResult.relationshipTypeIdsNum > 0) {
                        console.log(`neo4j server_${serId} 获取sysInfo: ` + JSON.stringify(systemResult));
                        logger.info(`neo4j server_${serId} 获取sysInfo: ` + JSON.stringify(systemResult));
                        break;
                    } else {
                        console.log(`neo4j server_${serId} 第${retryCount}次重新获取sysInfo`);
                        logger.info(`neo4j server_${serId} 第${retryCount}次重新获取sysInfo`);
                        sleep(waitGetSysInfoTime);
                        retryCount++;
                    }
                } catch (err) {
                    console.error(err);
                    logger.error(err);
                    retryCount++;
                }
            } while (retryCount < maxWaitSysInfoTimes)
            if (retryCount == maxWaitSysInfoTimes) {
                console.error(`neo4j server_${serId} ${retryCount}次获取sysInfo失败, neo4j server数据导入失败或启动失败！`);
                logger.error(`neo4j server_${serId} ${retryCount}次获取sysInfo失败, neo4j server数据导入失败或启动失败！`);
            }
        }
        if (systemResult.nodeIdsNum > 0 && systemResult.propertyIdsNum > 0 && systemResult.relationshipIdsNum > 0 && systemResult.relationshipTypeIdsNum > 0) {
            let createIndexRes1 = await transactions.startCreateIndex(serId, labelName1, indexName1);
            if (createIndexRes1) {
                console.log(`neo4j server_${serId} create index:${labelName1}(${indexName1}) success!`);
                logger.info(`neo4j server_${serId} create index:${labelName1}(${indexName1}) success!`);
                importDataResult.createIndexStatus = createIndexRes1;
            } else if (!createIndexRes1) {
                console.log(`neo4j server_${serId} create index:${labelName1}(${indexName1}) failure!`);
                logger.info(`neo4j server_${serId} create index:${labelName1}(${indexName1}) failure!`);
                importDataResult.createIndexStatus = null;
            }
            importDataResult.systemInfo = systemResult;
        }
        return importDataResult;
    } catch (err) {
        console.error(err);
        logger.error(err);
        return err;
    }
}

//远程控制服务器的防火墙开启和关闭
// async function controlServerFirewall(server, flag) {
//     try {
//         let result = false;
//         let serConInfo = {};
//         serConInfo.host = server.ip[0];
//         serConInfo.username = server.username;
//         serConInfo.password = server.password;
//         let actionCmd = null;
//         let startFlag = null;
//         let stopFlag = null;

//         if (flag == 'start') {
//             //start firewall
//             actionCmd = config.firewallSettingInfo.startFirewallCmd;
//             await executeShell.startExecuteCmd(serConInfo, actionCmd);
//             //check firewall state
//             actionCmd = config.firewallSettingInfo.getStatusCmd;
//             startFlag = await executeShell.startExecuteCmd(serConInfo, actionCmd);
//             if (startFlag === 'running') {
//                 console.log(`server_${server.id} start the firewall success!`);
//                 logger.info(`server_${server.id} start the firewall success!`);
//                 result = true;
//             }
//             else {
//                 console.error(`server_${server.id} start the firewall failure!`);
//                 logger.error(`server_${server.id} start the firewall failure!`);
//             }
//         }
//         else if (flag == 'stop') {
//             //stop firewall
//             actionCmd = config.firewallSettingInfo.stopFirewallCmd;
//             await executeShell.startExecuteCmd(serConInfo, actionCmd);
//             //check firewall state
//             actionCmd = config.firewallSettingInfo.getStatusCmd;
//             startFlag = await executeShell.startExecuteCmd(serConInfo, actionCmd);
//             if (startFlag === 'not running' || startFlag.indexOf('not running') >= 0) {
//                 console.log(`server_${server.id} stop the firewall success!`);
//                 logger.info(`server_${server.id} stop the firewall success!`);
//                 result = true;
//             }
//             else {
//                 console.error(`server_${server.id} stop the firewall failure!`);
//                 logger.error(`server_${server.id} stop the firewall failure!`);
//             }
//         }
//         return result;
//     }
//     catch (err) {
//         console.error(err);
//         logger.error(err);
//         return err;
//     }
// }

let apiHandlers = {

    // 定时触发全量更新数据
    timingUpdateTotalData: async function (flag) {
        return new Promise(async (resolve, reject) => {
            try {
                if (flag == 'true') {
                    let ctx = {};
                    let compId = `${config.updateInfo.companyId}`;
                    let relId_invest = `${config.updateInfo.relationId_invest}`;
                    let relId_guarantee = `${config.updateInfo.relationId_guarantee}`;
                    let upCompFlag = 'true';
                    let upRelFlag = 'true';
                    //let localShellPath = path.join(process.cwd(), '\\shell\\');             //windows
                    let localShellPath = path.join(process.cwd(), '/shell/');             //Ubuntu
                    let CSVFileNames = [];
                    CSVFileNames = config.sshConfig.csvFileName;
                    //let localCSVPath = path.join(process.cwd(), '\\totalData\\');           //windows
                    let localCSVPath = path.join(process.cwd(), '/totalData/');          //Ubuntu

                    //test for memoryUsage, 数据更新之前的memory
                    let memoryUsage1 = process.memoryUsage();
                    console.log('数据更新之前的memory: ' + JSON.stringify(memoryUsage1));
                    logger.info('数据更新之前的memory: ' + JSON.stringify(memoryUsage1));

                    //遍历存储每个服务器的信息
                    let servers = [];
                    let serverNum = config.sshServer.length;
                    if (serverNum > 0) {
                        for (let i = 0; i < serverNum; i++) {
                            let serverInfo = {};
                            serverInfo.id = config.sshServer[i].id;
                            serverInfo.ip = config.sshServer[i].ips;                                             //互为切换的服务器IP信息
                            serverInfo.port = config.sshServer[i].port;
                            serverInfo.username = config.sshServer[i].username;
                            serverInfo.password = config.sshServer[i].password;
                            serverInfo.remoteCSVPath = config.sshConfig.csvFilePath[i];                         //每台服务器上的csv文件路径
                            serverInfo.shellFileName = config.sshConfig.shellFileName[i];                       //要执行shell脚本的名称  
                            serverInfo.remoteShellPath = config.sshConfig.shellFilePath[i];                     //要执行shell脚本的路径
                            serverInfo.neo4jInstallPath = config.sshConfig.neo4jInstallPath[i];                 //neo4j server安装路径 
                            serverInfo.neo4jServerBoltPort = config.sshServer[i].boltPort;                      //neo4j server的bolt协议端口   
                            servers.push(serverInfo);
                            console.log(`服务器${serverInfo.id}的信息: ` + 'ip: ' + serverInfo.ip + ', remoteCSVPath: ' + serverInfo.remoteCSVPath + ', shellFileName: ' + serverInfo.shellFileName + ', remoteShellPath: ' + serverInfo.remoteShellPath + ', neo4jInstallPath: ' + serverInfo.neo4jInstallPath);
                            logger.info(`服务器${serverInfo.id}的信息: ` + 'ip: ' + serverInfo.ip + ', remoteCSVPath: ' + serverInfo.remoteCSVPath + ', shellFileName: ' + serverInfo.shellFileName + ', remoteShellPath: ' + serverInfo.remoteShellPath + ', neo4jInstallPath: ' + serverInfo.neo4jInstallPath);
                        }
                    } else {
                        console.error(errorResp(errorCode.INTERNALERROR), 'SSH server信息配置出错！');
                        logger.error(errorResp(errorCode.INTERNALERROR), 'SSH server信息配置出错！');
                        // return false;
                        return resolve(false);
                    }

                    ctx.last = 0;
                    ctx.updatetime = Date.now();
                    ctx.latestUpdated = 0;

                    transactions.saveContext(compId, ctx);
                    transactions.saveContext(relId_invest, ctx);
                    transactions.saveContext(relId_guarantee, ctx);

                    //1. for test 
                    // let compUpdateLast = { 'last': 0 };
                    // let investRelUpdateLast = { 'last': 0 };
                    // let guaranteeRelUpdateLast = { 'last': 0 };

                    let compUpdateLast = await transactions.getContext(compId);
                    let investRelUpdateLast = await transactions.getContext(relId_invest);
                    let guaranteeRelUpdateLast = await transactions.getContext(relId_guarantee);
                    console.log('初始化数据更新记录标识：' + 'compUpdateLast=' + compUpdateLast.last + ', investRelUpdateLast=' + investRelUpdateLast.last + ', guaranteeRelUpdateLast=' + guaranteeRelUpdateLast.last);
                    logger.info('初始化数据更新记录标识：' + 'compUpdateLast=' + compUpdateLast.last + ', investRelUpdateLast=' + investRelUpdateLast.last + ', guaranteeRelUpdateLast=' + guaranteeRelUpdateLast.last);
                    if (compUpdateLast.last == 0 && investRelUpdateLast.last == 0 && guaranteeRelUpdateLast.last == 0) {                                             //判断companies/relations更新数据的信息标识是否全部初始化
                        let now = Date.now();

                        //2. for test
                        // let updateRes = { 'compUpdateStatus': { 'status': 1 }, 'invRelUpdateStatus': { 'status': 1 }, 'guaRelUpdateStatus': { 'status': 1 } };
                        let updateRes = await timingUpdate.startTotalUpdate(upCompFlag, upRelFlag);
                        let loadCSVCost = Date.now() - now;
                        console.log('生成companies/relations的CSV文件耗时: ' + loadCSVCost + 'ms');
                        logger.info('生成companies/relations的CSV文件耗时: ' + loadCSVCost + 'ms');

                        //test for memoryUsage, 数据更新之后的memory
                        let memoryUsage2 = process.memoryUsage();
                        console.log('数据更新之后的memory: ' + JSON.stringify(memoryUsage2));
                        logger.info('数据更新之后的memory: ' + JSON.stringify(memoryUsage2));

                        //数据更新前后的memory变化
                        let rss_change1 = memoryUsage1.rss - memoryUsage2.rss;
                        let heapTotal_change1 = memoryUsage1.heapTotal - memoryUsage2.heapTotal;
                        let heapUsed_change1 = memoryUsage1.heapUsed - memoryUsage2.heapUsed;
                        let external_change1 = memoryUsage1.external - memoryUsage2.external;
                        let memoryChange1 = { rss_c: rss_change1, heapTotal_c: heapTotal_change1, heapUsed_c: heapUsed_change1, external_c: external_change1 };
                        console.log('数据更新前后的memory变化: ' + JSON.stringify(memoryChange1));
                        logger.info('数据更新前后的memory变化: ' + JSON.stringify(memoryChange1));

                        if (updateRes.compUpdateStatus.status == 1 && updateRes.invRelUpdateStatus.status == 1 && updateRes.guaRelUpdateStatus.status == 1) {        //判断companies/relations的CSV文件是否全部生成
                            let putCSVFlag = 0;                                                                       //记录CSV文件传输成功和失败的信息
                            let putShellFlag = 0;                                                                     //记录Shell文件传输成功和失败的信息
                            //SSH远程传输CSV文件
                            for (let ser of servers) {
                                let remoteCSVPath = ser.remoteCSVPath;
                                for (CSVFileName of CSVFileNames) {

                                    //3. for test
                                    // let putCSVRes = { ok: 1, putFilesMsg: 'success' };                                                                                  //分别SSH传输companies.csv和relations.csv

                                    let putCSVRes = await executeShell.startPutFiles(ser, CSVFileName, localCSVPath, remoteCSVPath);
                                    if (putCSVRes.hasOwnProperty('ok') && putCSVRes.ok == 1) {
                                        console.log('SSH传输CSV文件信息：' + putCSVRes.putFilesMsg);
                                        logger.info('SSH传输CSV文件信息：' + putCSVRes.putFilesMsg);
                                        putCSVFlag += putCSVRes.ok;
                                    } else {
                                        console.error('执行executeShell.startPutCSVFiles失败！');
                                        logger.error('执行executeShell.startPutCSVFiles失败！');
                                    }
                                }
                            }
                            //SSH远程传输Shell文件
                            for (let ser of servers) {
                                let shellFileName = ser.shellFileName;
                                let remoteShellPath = ser.remoteShellPath;

                                //4. for test
                                // let putShellRes = { ok: 1 };

                                let putShellRes = await executeShell.startPutFiles(ser, shellFileName, localShellPath, remoteShellPath);
                                if (putShellRes.hasOwnProperty('ok') && putShellRes.ok == 1) {
                                    console.log('SSH传输Shell文件信息：' + putShellRes.putFilesMsg);
                                    logger.info('SSH传输Shell文件信息：' + putShellRes.putFilesMsg);
                                    putShellFlag += putShellRes.ok;
                                } else {
                                    console.error('执行executeShell.startPutShellFiles失败！');
                                    logger.error('执行executeShell.startPutShellFiles失败！');
                                }
                            }
                            let serNum = servers.length;
                            let csvFileNum = config.sshConfig.csvFileName.length;                                     //生成的CSV文件数目
                            if (putCSVFlag == serNum * csvFileNum && putShellFlag == serNum) {                        //判断所有服务器的文件是否传输成功
                                for (let ser of servers) {
                                    let serId = ser.id;
                                    let shellFileName = ser.shellFileName;
                                    let remoteShellPath = ser.remoteShellPath;
                                    let neo4jInstallPath = ser.neo4jInstallPath;
                                    let serConInfo = { host: ser.ip[0], port: ser.port, username: ser.username, password: ser.password };
                                    let stateCmd = `echo "show servers state" | socat stdio tcp4-connect:${HAServerIP}:${HAServerPort}`;

                                    //5. for test
                                    // let importDataRes = { systemInfo: { nodeIdsNum: 1000, propertyIdsNum: 2200, relationshipIdsNum: 3, relationshipTypeIdsNum: 3 } };
                                    let importDataRes = await startImportTotalData(ser, shellFileName, remoteShellPath);
                                    if (importDataRes.systemInfo) {
                                        let nodeIdsNum = importDataRes.systemInfo.nodeIdsNum;
                                        let propertyIdsNum = importDataRes.systemInfo.propertyIdsNum;
                                        let relationshipIdsNum = importDataRes.systemInfo.relationshipIdsNum;
                                        let relationshipTypeIdsNum = importDataRes.systemInfo.relationshipTypeIdsNum;
                                        if (nodeIdsNum > 0 && propertyIdsNum > 0 && relationshipIdsNum > 0 && relationshipTypeIdsNum > 0) {
                                            console.log(`neo4j server_${serId} 执行完startImportTotalData后, 等待创建索引时间: ` + waitIndexOnTime + 'ms');
                                            logger.info(`neo4j server_${serId} 执行完startImportTotalData后, 等待创建索引时间: ` + waitIndexOnTime + 'ms');
                                            let indexState = null;
                                            let retryCount = 0;
                                            do {
                                                try {
                                                    /*
                                                    //开启防火墙, server端口不通, 使得负载不会落在无索引的server 上
                                                    let startFlag = await controlServerFirewall(ser, 'start');
                                                    if (startFlag == true) {
                                                        console.log('start firewall success!');
                                                        logger.info('start firewall success!');
                                                        // sleep(waitIndexOnTime);

                                                        //6. for test
                                                        sleep(10000);
                                                    }
                                                    else {
                                                        console.error('start firewall failure!');
                                                        logger.error('start firewall failure!');
                                                        break;
                                                    }

                                                    //关闭防火墙, server端口通
                                                    let stopFlag = await controlServerFirewall(ser, 'stop');
                                                    if (stopFlag == true) {
                                                        console.log('stop firewall success!');
                                                        logger.info('stop firewall success!');
                                                    }
                                                    else {
                                                        console.error('stop firewall failure!');
                                                        logger.error('stop firewall failure!');
                                                        break;
                                                    }
                                                    //确认防火墙关闭后, 检查index的状态
                                                    */

                                                    //建索引时，先set server state maint, 建完索引后set server state ready, 0代表The server is down，2代表The server is fully up.
                                                    hs.backend(HAServerName).server(`server_${serId}`).address(ser.ip[0], ser.neo4jServerBoltPort);
                                                    hs.backend(HAServerName).server(`server_${serId}`).disable();                        //set server state maint
                                                    hs.backend(HAServerName).server(`server_${serId}`).shutdownSession();                //Immediately terminate all the sessions attached to the specified server.
                                                    hs.backend(HAServerName).server(`server_${serId}`).showState();

                                                    //6. for test
                                                    // sleep(180000);
                                                    sleep(waitIndexOnTime);

                                                    hs.backend(HAServerName).server(`server_${serId}`).address(ser.ip[0], ser.neo4jServerBoltPort);
                                                    hs.backend(HAServerName).server(`server_${serId}`).enable();                        //set server state ready
                                                    hs.backend(HAServerName).server(`server_${serId}`).showState();

                                                    indexState = await transactions.getIndexStatus(serId, labelName1, indexName1);

                                                    if (indexState == 'ONLINE') {
                                                        console.log(`第${retryCount}次查询index, indexState: ` + indexState);
                                                        logger.info(`第${retryCount}次查询index, indexState: ` + indexState);
                                                        break;
                                                    }
                                                    else if (indexState == 'POPULATING') {
                                                        retryCount++;
                                                        console.log(`第${retryCount}次查询index, indexState: ` + indexState);
                                                        logger.info(`第${retryCount}次查询index, indexState: ` + indexState);
                                                    }
                                                } catch (err) {
                                                    console.error(err);
                                                    logger.error(err);
                                                    retryCount++;
                                                }
                                            } while (retryCount < maxWaitIndexOnTimes)
                                            if (retryCount == maxWaitIndexOnTimes) {
                                                console.error(`等待创建索引次数: ${maxWaitIndexOnTimes}, 最终创建索引失败!`);
                                                logger.error(`等待创建索引次数: ${maxWaitIndexOnTimes}, 最终创建索引失败!`);
                                                break;
                                            }
                                        } else {
                                            console.error(`neo4j server_${serId} 执行startImportTotalData失败！停止执行程序！`);
                                            logger.error(`neo4j server_${serId} 执行startImportTotalData失败！停止执行程序！`);
                                            break;
                                        }
                                    }
                                    else if (!importDataRes.systemInfo) {
                                        console.error(`neo4j server_${serId} 执行startImportTotalData失败！停止执行程序！`);
                                        logger.error(`neo4j server_${serId} 执行startImportTotalData失败！停止执行程序！`);
                                        break;
                                    }

                                    //for test ,测试设置60s
                                    // sleep(60000);
                                }
                            } else {
                                // return false;
                                return resolve(false);
                            }
                        } else {
                            // return false;
                            return resolve(false);
                        }
                    } else {
                        // return false;
                        return resolve(false);
                    }
                    // return true;
                    return resolve(true);
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                // return err;
                return reject(err);
            }

            //for test
            // return resolve(true);
        })

    },

    //外部接口触发增量更新数据
    // directUpdateData: async function (request, reply) {
    directUpdateData: async function (updateRelFlag) {

        if (!updateRelFlag) updateRelFlag = config.updateFlag.isUpdateIncreRel;
        try {
            let increUpdateResult = {};
            let now = Date.now();
            let updateRes = {};

            // updateRes = await timingUpdate.startIncreUpdate(updateRelFlag);
            increUpdateResult.updateIncreDataInfo = updateRes;
            //for test
            let updateRecord = 5;
            // let updateRecord = updateRes.relAddUpdateStatus.loadDataInfo.dataLines;
            console.log('incrementalUpdateStatus: update relations successfully!');
            logger.info('incrementalUpdateStatus: update relations successfully!');

            let ips = config.sshServer.ips;
            let localPath = '';
            let remotePath = config.sshConfig.csvFilePath;
            let putFileRes = {};

            // //传输incrementalRelations.csv文件至远程服务器
            let incrementalRelationsFileRes = [];
            // localPath = path.join(process.cwd(), '\\incrementalData\\');                 //windows环境, 本地保存增量更新 relations CSV文件的路径
            localPath = path.join(process.cwd(), '/incrementalData/');                       //Ubuntu环境，本地保存增量更新 relations CSV文件的路径

            if (updateRecord == 0) {
                incrementalRelationsFileRes.push('no incrementalRelations data will update!');
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' no incrementalRelations data will update!');
                logger.info("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' no incrementalRelations data will update!');
            } else if (updateRecord > 0) {
                let now = Date.now();
                let incrementalRelationsFileName = `incrementalRelations.csv`;
                // if (!relFileName) continue;
                incrementalRelationsFileRes.push(await executeShell.startPutFiles(ips, incrementalRelationsFileName, localPath, remotePath));
                let putIncrementalRelationsFileCost = Date.now() - now;
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` 远程传输: incrementalRelations.csv 耗时: ` + putIncrementalRelationsFileCost + 'ms');
                logger.info(`remote transmission: incrementalRelations.csv costs: ` + putIncrementalRelationsFileCost + 'ms');
            }
            putFileRes.relAddFileMsg = incrementalRelationsFileRes;
            increUpdateResult.putFileInfo = putFileRes;

            //执行增量更新relations的 load CSV 语句
            let loadCSVRes = {};
            let loadRelCSVRes = [];
            let fileName = '';

            //import incrementalRelations.csv
            if (updateRecord == 0) {
                loadRelCSVRes.push(' no incrementalRelations data will import!');
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' no incrementalRelations data will import!');
                logger.info("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' no incrementalRelations data will import!');
            } else if (updateRecord > 0) {
                let now = Date.now();
                fileName = 'incrementalRelations';
                loadRelCSVRes.push(await transactions.startLoadInreRelCSV(fileName));
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `import ${fileName}.csv successfully!`);
                logger.info(`import ${fileName}.csv successfully!`);
                let loadRelationsCSVCost = Date.now() - now;
                console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` 执行Load CSV: ${fileName}.csv 耗时: ` + loadRelationsCSVCost + 'ms');
                logger.info(`Load ${fileName}.csv costs: ` + loadRelationsCSVCost + 'ms');
            }

            loadCSVRes.loadRelAddCSVMsg = loadRelCSVRes;
            increUpdateResult.loadCSVInfo = loadCSVRes;
            console.log('increUpdateResult: import inremental relations successfully!');
            // return reply.response({ok: 1, increUpdateResult: increUpdateResult});

        } catch (err) {
            console.log(err);
            logger.info('err: ' + err);
            return ({});
        }
    },

    //创建索引
    createIndex: async function (request, reply) {
        let label = request.query.labelName;
        let index = request.query.indexName;
        if (!label) label = config.nodeLabelName1;
        if (!index) index = config.nodeIndexName1;
        let res = {};
        try {
            res = await transactions.startCreateIndex(label, index);
            return reply.response({ ok: 1, message: res });
        } catch (err) {
            return reply.response(err);
        }
    },

    //删除索引
    deleteIndex: async function (request, reply) {
        let label = request.query.labelName;
        let index = request.query.indexName;
        if (!label) label = config.nodeLabelName1;
        if (!index) index = config.nodeIndexName1;
        let res = {};
        try {
            res = await transactions.startDeleteIndex(label, index);
            return reply.response({ ok: 1, message: res });
        } catch (err) {
            return reply.response(err);
        }
    },

    //删除lockResource
    deleteLockResource: async function (request, reply) {
        try {
            let lockResource = request.query.lockResource;
            if (!lockResource) {
                lockResource = config.lockInfo.resource[0];
            }
            redis.del(lockResource);
            redis1.del(lockResource);
            return reply.response({ ok: 1, message: `delete the lock resource: '${lockResource}' succeess!` })

        } catch (err) {
            return reply.response(err);
        }
    },

    //控制防火墙的开关
    // controlFirewall: async function (request, reply) {
    //     try {
    //         let serverId =  request.query.serverId;
    //         let action = request.query.action;
    //         let serverInfo = null;
    //         let actionInfo = null;
    //         if (!serverId) {
    //             console.log('serverId is null');
    //             logger.info('serverId is null');
    //             return reply.response({ok: 1, message: 'serverId is null, please fill in!'});
    //         } 
    //         else if (serverId && serverId == 'a') {
    //             serverInfo = remoteServersInfo[0];
    //         }
    //         else if (serverId && serverId == 'b') {
    //             serverInfo = remoteServersInfo[1];
    //         }
    //         if (!action) {
    //             console.log('action is null');
    //             logger.info('action is null');
    //             return reply.response({ok: 1, message: 'action is null, please fill in!'});
    //         }
    //         else if (action && action == 'start') {
    //             actionInfo = config.firewallSettingInfo.startFirewallCmd;
    //         }
    //         else if (action && action == 'stop') {
    //             actionInfo = config.firewallSettingInfo.stopFirewallCmd;
    //         }
    //         else if (action && action == 'state') {
    //             actionInfo = config.firewallSettingInfo.getStatusCmd;
    //         }
    //         if (serverInfo && actionInfo) {
    //             // serverInfo = {host: '10.10.15.27', username: 'root', password: 'tcdept'};
    //             let cmdResult = await executeShell.startExecuteCmd(serverInfo, actionInfo);
    //             // let now = new Date();
    //             // sleep(10000);
    //             // let sleepTime = new Date() - now;
    //             // console.log('sleep: ' +sleepTime);
    //             console.log(`execute the cmd: '${actionInfo}' result: ` +cmdResult);
    //             return reply.response({ok: 1, message: cmdResult});
    //         }
    //         else if (serverInfo || actionInfo) {
    //             return reply.response({ok: 1, message: 'get the remote servers config info failure!'});
    //         }

    //     } catch (err) {
    //         return reply.response(err);
    //     }
    // },

    //for test, 获取neo4j sysinfo
    querySystemInfo: async function (request, reply) {
        let serId = request.query.serverId;
        let res = null;
        try {
            res = await transactions.getSystemInfo(serId);
            return reply.response({ ok: 1, message: res });
        } catch (err) {
            return reply.response(err);
        }
    },

    //test for updating total data
    updateTotalData: async function (request, reply) {
        try {
            redlock.lock(lockResource, lockTTL).then(async function (lock) {
            let updateFlag = await apiHandlers.timingUpdateTotalData('true');
            if (updateFlag == true || updateFlag == false) {
                // unlock your resource when you are done
                return lock.unlock()
                    .catch(function (err) {
                        // we weren't able to reach redis; your lock will eventually
                        // expire, but you probably want to log this error
                        console.error(err);
                        logger.error(err);
                    });
            }
            })
            return reply.response({ ok: 1, message: 'updating total data...' });
        } catch (err) {
            return reply.response(err);
        }
    },

    //test for executeShellCmd
    executeShellCmd: async function () {
        let remoteShellPath = '/home/neo4j/';
        let shellFileName = 'importTotalData_b.sh';
        // let cmd = `cd ${remoteShellPath} && chmod +777 ./${shellFileName} && ./${shellFileName}`;
        let cmd = `cd ${remoteShellPath} && chmod +x ./${shellFileName} && ./${shellFileName}`;
        let server = { 'ip': ['10.10.15.27'], 'port': 22, 'username': 'root', 'password': 'tcdept' };
        let cmdRes = await executeShell.startCmdShell(cmd, server);
        console.log('cmd result ok= ' + cmdRes.ok);
        let nodeNumber = 0;
        if (cmdRes.cmdResultDetail) {                                                     //判断shellCmd 是否执行成功，即neo4j server 是否导入数据成功
            let getNodeRes = await transactions.getNodeNum('b');                        //获取neo4j server存储的node节点数目
            nodeNumber = getNodeRes;
            console.log('nodeNum: ' + getNodeRes);
        }
        if (nodeNumber != 0) {
            let createIndexRes1 = await transactions.startCreateIndex('b', 'company', 'ITCode2');
        }

    },

    //test for get index status
    indexStatus: async function (request, reply) {
        try {
            let indexState = await transactions.getIndexStatus('a', 'company', 'ITCode2');
            return reply.response({ ok: 1, indexState: indexState });
        } catch (err) {
            return reply.response(err);
        }

    }

}

//for test
async function checkIndexState() {
    let indexState = await transactions.getIndexStatus('a', 'company', 'ITCode2');
    console.log('getIndexStatus被执行, indexState: ' + indexState);
    return indexState;

}

//单次在neo4j server上执行一次shell命令
async function execEachServer(ser) {
    try {

        let serId = ser.id;
        let shellFileName = ser.shellFileName;
        let remoteShellPath = ser.remoteShellPath;
        let neo4jInstallPath = ser.neo4jInstallPath;
        // let checkSerStatusCmd = `ps aux|grep ${neo4jInstallPath}`;
        let importDataRes = await startImportTotalData(ser, shellFileName, remoteShellPath);
        // let getNodeRes =  await transactions.getNodeNum();                                      //获取neo4j server存储的node节点数目
        let nodeNumber = 0;
        if (importDataRes.cmdShellResult.cmdResultDetail) {                                         //判断shellCmd 是否执行成功，即neo4j server 是否导入数据成功
            let getNodeRes = await transactions.getNodeNum(serId);                                    //获取neo4j server存储的node节点数目
            nodeNumber = getNodeRes;
            console.log(`neo4j server_${serId} nodeNum: ` + getNodeRes);
            logger.info(`neo4j server_${serId} nodeNum: ` + getNodeRes);
            // if (nodeNumber != 0) {
            //     let indexAddStatus = forImmediate('POPULATING'); 
            //     if (indexAddStatus == 'ONLINE') continue;
            // }                                                               
            if (nodeNumber == 0) {
                console.error(`neo4j server_${serId} is shut down, it will stop updating data! `);
                logger.error(`neo4j server_${serId} is shut down, it will stop updating data! `);
                // break;
            }
        }

    } catch (err) {
        console.error(err);
        logger.error(err);
    }

}




module.exports = apiHandlers;