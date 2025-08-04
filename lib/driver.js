
import net from "node:net"
import PacketParser from "./packetparser.js";
import tls from "node:tls"
const QUEUALREADYEXIST = 126
const ERROR = 126
const SUCCESS = 127
const NOOPTS = 0x00
const JSONENCODED = 0x01
const CREATE = 0x1
const READ = 0x2
const PUBLISH = 0x3
const ACK = 0x4
const OPTS = 0x5
const NEWQUEUE = 0x6
const SUBSCRIBE = 0x7
const HANDSHAKE = 0x7B
const AUTHENTICATE = 0x7a
const PROTOCOL_VERSION = 0x0001
const HEARTBEAT_SIGNAL = -1

const DEFAULT_RECONNECT_DELAY = 5000;
const MAX_RECONNECT_ATTEMPTS = 5;


export default class Bunnymq {

    /**
     * 
     * @param {{port, host, username, password}} config 
     */
    constructor(config) {
        this.stream = undefined
        this._command = null;
        this._commands = []
        this._id = ""
        this.clientId = ""
        this._queue = ""
        this._consumer = undefined  // TODO: support multiple consumers Map(queuename, consumer)
        this.config = config
        this.connectionState = "disconnected"
        this.isWritable = true

        this.packetParser = new PacketParser((packet) => {
            this.handlePacket(packet)
        })

        this.connect()
    }

    connect() {
        try {
            this.connectionState = "connecting";

            if (this.config.tls) {
                const tlsOpts = {
                    ...(this.config.tlsOptions || {}),
                    rejectUnauthorized: false
                };
                this.stream = tls.connect(this.config.port, this.config.host, tlsOpts, () => {
                    this.handleConnect();
                });
            } else {
                this.stream = net.createConnection(this.config.port, this.config.host, () => {
                    this.handleConnect();
                });
            }

            this.setupStreamHandlers();
        } catch (error) {
            this.handleConnectionError(`Connection failed: ${error.message}`);
        }
    }

    sendHandshake() {
        const handshakeBuffer = Buffer.alloc(2);
        handshakeBuffer.writeInt8(HANDSHAKE, 0);
        handshakeBuffer.writeInt8(PROTOCOL_VERSION, 1);
        this.sendToStream(handshakeBuffer);
    }

    handleReconnect() {
        if (!this.config.autoReconnect || this.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            this.handleError("Max reconnect attempts reached. Giving up.");
            return;
        }

        this.reconnectAttempts++;
        const delay = DEFAULT_RECONNECT_DELAY * this.reconnectAttempts;

        setTimeout(() => {
            this.handleError(`Attempting reconnect (${this.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
            this.connect();
        }, delay);
    }


    handleConnect() {
        this.connectionState = "connected";
        this.reconnectAttempts = 0;
        this.isWritable = true;
        this.sendHandshake();
    }

    setupStreamHandlers() {
        this.stream.setKeepAlive(true, 10000);
        this.stream.setNoDelay(true);

        this.stream.on("data", data => {
            try {
                this.packetParser.execute(data)
            } catch (error) {
                this.handleError(`Data processing error: ${error.message}`);
            }
        });

        this.stream.on("drain", () => {
            this.isWritable = true;
        });

        this.stream.on("error", error => {
            this.handleConnectionError(`Connection error: ${error.message}`);
        });

        this.stream.on("close", () => {
            this.connectionState = "disconnected";
            this.handleReconnect();
        });

        this.stream.on("end", () => {
            this.connectionState = "disconnected";
        });
    }

    handleError(message) {
        console.error(`[BunnyMQ Error] ${message}`);
        // TODO: error handling logic here
    }

    cleanupStream() {
        if (this.stream) {
            this.stream.removeAllListeners();
            if (!this.stream.destroyed) {
                this.stream.destroy();
            }
            this.stream = null;
        }
        this.isWritable = false;
    }

    handleConnectionError(message) {
        this.handleError(message);
        this.cleanupStream();
        this.handleReconnect();
    }

    sendCredentials() {
        const credentials = Buffer.from(`${this.config.username}:${this.config.password}`, "utf-8");

        const header = Buffer.alloc(5);
        header.writeInt8(AUTHENTICATE, 0);
        header.writeInt32BE(credentials.length, 1);

        const authBuffer = Buffer.concat([
            header,
            credentials,
            Buffer.from(this.clientId)
        ]);

        this.sendToStream(authBuffer);
    }


    /**
     * @param {Buffer} data
     */
    sendToStream(data) {
        if (!this.stream || this.stream.destroyed) {
            this.handleError("Attempted write to closed connection");
            return false;
        }

        if (this.isWritable) {
            this.isWritable = this.stream.write(data);
            return this.isWritable;
        } else {
            this.writeBuffer.push(data);
            return false;
        }
    }


    handleCommandResponse(packet) {
        // console.log(this._command, packet)
        if (!this._command) return;
        switch (this._command.type) {
            case "QueueDeclare":
                // console.log("QueueDeclare")
                if (packet.res == SUCCESS || packet.res == QUEUALREADYEXIST) {
                    this._command.cb(packet.res)
                } else {
                    this._command.cb("Queue declaration failed: ", packet.res)
                }
                break;
            case "Publish":
                if (packet.res == ERROR) {
                    this._command.cb(new Error(`Publish ERROR: ${this._command.payload}`))
                } else {
                    this._command.cb(`success :${packet.res} message: ${this._command.payload}`)
                }
                break;
            case "Ack":
                if (packet.res == SUCCESS) {
                    this._command.cb(true)
                } else
                    this._command.cb(false)
                break;
            case "Consume":
                break;
            default:
                break;
        }

        this.nextCmd()
    }
    handleHeartBeat() {
        const heartbeat = Buffer.alloc(1);
        heartbeat.writeInt8(HEARTBEAT_SIGNAL, 0);
        this.sendToStream(heartbeat);
    }
    handlePacket(packet) {
        try {
            switch (packet.type) {
                case 'heartbeat':
                    this.handleHeartBeat()
                    break;
                case "msg":
                  console.log(packet, "consuming")  
                  this._consumer(packet.msg)
                    
                    break;
                case "HANDSHAKE":
                    this.clientId = packet.id;
                    this.sendCredentials();
                    break;
                case "AUTHENTICATED":
                    if (packet.status == 200) {
                        this.nextCmd()
                    } else {
                        throw new Error(`${packet.errorMsg}`)
                    }
                    break;

                default:
                    this.handleCommandResponse(packet)
                    break;
            }


        } catch (error) {
            this.handleError(`Packet handling error: ${error.message}`);
        }
    }

    nextCmd() {
        this._command = this._commands.shift()
        if (!this._command) return;
        // console.log(this._command, "current command");
        this.executeCmd()
    }



    queueDeclare(opts, callback) {
        this.addCommand({ type: "QueueDeclare", opts, cb: callback })
    }
    /**
     * 
     * @param { {name: string,  config: {QueueExpiry: number,MessageExpiry: number ,AckExpiry: number,Durable: boolean,noAck: boolean}}} opts 
     * @param {()=> void}
     */
    #QueueDeclare(opts) {
        // if (this._command && this._command.type != "QueueDeclare") {
        //     console.log(this._command.type)
        //     this.addCommand({ type: "QueueDeclare", opts, cb })
        //     return;
        // }

        // if (!this._command) {
        //     this.addCommand({ type: "QueueDeclare", opts, cb })
        // }

        this._queue = opts.name
        const queuename = Buffer.from(this._queue, "utf-8")
        const opts_ = Buffer.from(JSON.stringify(opts.config == undefined ? {} : opts.config), "utf-8")
        const op = Buffer.alloc(6);
        op.writeInt8(NEWQUEUE, 0); // Send a "health check" opcode
        op.writeInt8(opts.config == undefined ? NOOPTS : JSONENCODED, 1);
        op.writeUint32BE(queuename.length, 2)
        const combinedBuffer = Buffer.concat([op, queuename, opts_])
        this.sendToStream(combinedBuffer)

    }

    publish(queue, payload, callback) {
        this.addCommand({ type: "Publish", payload, queue, cb: callback })
    }
    /**
     * 
     * @param {string} payload 
     * @param {() => void} cb 
     * @returns 
     */
    #Publish(payload, queue) {
        // if (this._command && this._command.type != "Publish") {
        //     this.addCommand({ type: "Publish", payload, cb })
        //     return;
        // }

        // if (!this._command) {
        //     this.addCommand({ type: "Publish", payload, cb })

        // }
        // console.log('Client connected.');
        const data = Buffer.from(payload, "utf-8")
        const meta = Buffer.from(JSON.stringify({ queue: queue || this._queue }), "utf-8")

        const op = Buffer.alloc(6);

        op.writeInt8(PUBLISH, 0);
        op.writeInt8(JSONENCODED, 1);
        op.writeUint32BE(data.length, 2)

        const combinedBuffer = Buffer.concat([op, data, meta])
        this.sendToStream(combinedBuffer)
    }

    consume(queuname, callback) {
        this._consumer = callback
        this.addCommand({ type: "Consume", queuname })
    }
    #Consume(queuname) {
        // if (this._command && this._command.type != "Consume") {
        //     this.addCommand({ type: "Consume", queuname, cb })
        //     return;
        // }

        // if (!this._command) {
        //     this.addCommand({ type: "Consume", queuname, cb })

        // }
        // console.log(this.clientId)
        const data = Buffer.from(this.clientId, "utf-8")
        const meta = Buffer.from(queuname, "utf-8")

        const op = Buffer.alloc(6);

        op.writeInt8(SUBSCRIBE, 0);
        op.writeInt8(NOOPTS, 1);
        op.writeUint32BE(data.length, 2)
        const combinedBuffer = Buffer.concat([op, data, meta])
        this.sendToStream(combinedBuffer)
    }

    ack(callback = () => { }) {
        this.addCommand({ type: "Ack", cb: callback })
    }
    #Ack() {

        const data = Buffer.from(this.clientId, "utf-8")
        const meta = Buffer.from("", "utf-8")

        const op = Buffer.alloc(6);

        op.writeInt8(ACK, 0);
        op.writeInt8(NOOPTS, 1);
        op.writeUint32BE(data.length, 2)
        const combinedBuffer = Buffer.concat([op, data, meta])
        // console.log(combinedBuffer, "combined")
        this.stream.write(combinedBuffer);
    }

    addCommand(cmd) {
        this._commands.push(cmd)
        if (!this._command && this.connectionState === "connected") {
            this.nextCmd();
        }
    }


    executeCmd() {
        switch (this._command.type) {
            case "QueueDeclare":
                this.#QueueDeclare(this._command.opts, this._command.cb)
                break;

            case "Publish":
                this.#Publish(this._command.payload, this._command.queue)
                break;
            case "Consume":

                this.#Consume(this._command.queuname)
                break;
            case "Ack":
                this.#Ack()
                break

            default:
                this.handleError(`Unkown Command ${this._command.type}`)
                break;
        }
    }
}