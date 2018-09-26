const Hapi = require('hapi');
const server = new Hapi.Server();
const apiHandlers = require("./dataUpdate/apiHandlers.js");
require('events').EventEmitter.prototype._maxListeners = 100;
const path = require('path');
const moment = require('moment');
const dumpFilePath = path.join(process.cwd(), '/heapDumpFile/');
console.log('dumpFilePath: ' +dumpFilePath);
// const heapdump = require('heapdump');
const dumpFileName = `${moment(Date.now()).format("YYYY-MM-DD--HH:mm:ss")}.heapsnapshot`;
// heapdump.writeSnapshot(dumpFilePath+dumpFileName, function(err, dumpFilePath) {
//     console.log('dump written to', dumpFileName);
//   });

server.connection({
   port: 8191,
   routes: {
       cors: true
   },
   compression: true
});


//test for execute shell cmd
server.route({
    method: 'GET',
    path: '/executeShellCmd',
    handler: apiHandlers.executeShellCmd,
    config: {
        timeout: {
          server: 100000
      }
    }
});


//创建索引
server.route({
    method: 'GET',
    path: '/createIndex',
    handler: apiHandlers.createIndex
});

//删除索引
server.route({
    method: 'GET',
    path: '/deleteIndex',
    handler: apiHandlers.deleteIndex
});

//查询索引信息
server.route({
    method: 'GET',
    path: '/indexStatus',
    handler: apiHandlers.indexStatus
});

//外部调用接口触发全量数据更新
server.route({
    method: 'GET',
    path: '/updateTotalData',
    handler: apiHandlers.updateTotalData
});

//外部调用接口获取neo4j sysinfo
server.route({
    method: 'GET',
    path: '/getSystemInfo',
    handler: apiHandlers.querySystemInfo
});

//外部调用接口删除lockResource
server.route({
    method: 'GET',
    path: '/deleteLockResource',
    handler: apiHandlers.deleteLockResource
});

//外部调用接口控制防火墙
// server.route({
//     method: 'GET',
//     path: '/controlFirewall',
//     handler: apiHandlers.controlFirewall
// });

//for test
server.route({
    method: 'GET',
    path: '/publishMessage',
    handler: apiHandlers.publishMessageToRedis
});

server.start((err) => {
    if (err) {
        // heapdump.writeSnapshot(dumpFilePath+dumpFileName, function(err, dumpFilePath) {
        //     console.log('dump written to', dumpFileName);
        //   });
        throw err;
    }
    console.info(`neo4j数据库全量数据更新API服务运行在:${server.info.uri}`);
});

process.on('unhandledRejection', (err) => {
    // heapdump.writeSnapshot(dumpFilePath+dumpFileName, function(err, dumpFilePath) {
    //     console.log('dump written to', dumpFileName);
    //   });
    console.log(err);
    console.log('NOT exit...');
    process.exit(1);
});

