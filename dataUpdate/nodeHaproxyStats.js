const req = require('require-yml');
const config = req('./config/source.yml');
const net = require('net');
const log4js = require('log4js');
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

class HAStats {
    constructor(ha_ip, ha_stats_port) {
        this._host = ha_ip;
        this._port = ha_stats_port;
    }

    backend(name) {
        this._backend = name;
        return this;
    }

    server(name) {
        this._serverName = name;
        return this;
    }

    address(ip, port) {
        console.log(`set server ${this._host} ${this._backend}/${this._serverName} addr ${ip} port ${port}`);
        logger.info(`set server ${this._host} ${this._backend}/${this._serverName} addr ${ip} port ${port}`);
        return this._apply(`set server ${this._backend}/${this._serverName} addr ${ip} port ${port}`);
    }

    enable() {
        // console.log(`set server ${this._backend}/${this._serverName} state ready`);
        // logger.info(`set server ${this._backend}/${this._serverName} state ready`);
        // return this._apply(`set server ${this._backend}/${this._serverName} state ready`);
        console.log(`enable server ${this._host} ${this._backend}/${this._serverName}`);
        logger.info(`enable server ${this._host} ${this._backend}/${this._serverName}`);
        return this._apply(`enable server ${this._backend}/${this._serverName}`);
    }

    disable() {
        // console.log(`set server ${this._backend}/${this._serverName} state maint`);
        // logger.info(`set server ${this._backend}/${this._serverName} state maint`);
        // return this._apply(`set server ${this._backend}/${this._serverName} state maint`);
        // return this._apply(`set server ${this._backend}/${this._serverName} state drain`);
        // return this._apply(`set server ${this._backend}/${this._serverName} agent down`);
        console.log(`disable server ${this._host} ${this._backend}/${this._serverName}`);
        logger.info(`disable server ${this._host} ${this._backend}/${this._serverName}`);
        return this._apply(`disable server ${this._backend}/${this._serverName}`);
    }

    shutdownSession() {
        console.log(`shutdown sessions server ${this._host} ${this._backend}/${this._serverName}`);
        logger.info(`shutdown sessions server ${this._host} ${this._backend}/${this._serverName}`);
        return this._apply(`shutdown sessions server ${this._backend}/${this._serverName}`);
    }    

    showState() {
        console.log(`server ${this._host} ${this._backend}/${this._serverName}: echo "show servers state" | socat stdio tcp4-connect:${this._host}:${this._port}`);
        logger.info(`server ${this._host} ${this._backend}/${this._serverName}: echo "show servers state" | socat stdio tcp4-connect:${this._host}:${this._port}`);
        return this._apply('show servers state');  
    }

    _apply(cmd) {
        let port = this._port;
        let host = this._host;
        let command = cmd;
        return new Promise((resolve, reject) => {
            const client = net.createConnection({ port: port, host: host }, () => {
                let execCmd = `${command}\n`;
                console.log(`${host}:${port} execute command:${execCmd}`);
                logger.info(`${host}:${port} execute command:${execCmd}`);
                client.write(execCmd);
            });
            client.on('data', (data) => {
                console.log(data.toString());
                logger.info(data.toString());
                return resolve();
                client.end();
            });
            client.on('end', () => {
                console.log(`${cmd}: disconnected from HAProxy`);
                logger.info(`${cmd}: disconnected from HAProxy`);
                return resolve();
            });
            client.on('error', (error) => {
                console.error(error);
                logger.error(error);
                return reject(error);
            });
        });
    }
}

module.exports = HAStats;

/*
使用：
let hs = new HAStats("10.15.44.229",9999);
* 切换backend到指定地址
hs.backend("servers").server('test1').address("10.15.44.133",8600);

* 禁用backend servers下的test1 server
hs.backend("servers").server('test1').disable();

* 启用backend servers下的test1 server
hs.backend("servers").server('test1').enable();
*/

// let hs = new HAStats("10.10.15.129",9999);

//切换backend到指定地址
// hs.backend("neo4j_servers").server('server_a').address("10.10.15.27",7687);

// hs.backend("neo4j_servers").server('server_a').enable();

// hs.backend("neo4j_servers").server('server_a').disable();
// hs.backend("neo4j_servers").server('server_a').shutdownSession();

// hs.backend("neo4j_servers").server('server_a').showState();



// hs.backend("neo4j_servers").server('server_b').address("10.10.17.10",7688);

// hs.backend("neo4j_servers").server('server_b').enable();

// hs.backend("neo4j_servers").server('server_b').disable();
// hs.backend("neo4j_servers").server('server_b').shutdownSession();

// hs.backend("neo4j_servers").server('server_b').showState();