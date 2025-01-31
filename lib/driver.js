
import net from "node:net"
import PacketParser from "./packetparser.js";

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


export default class Bunnymq {

    /**
     * 
     * @param {{port, host}} config 
     */
    constructor(config) {
        this.stream = net.createConnection(config.port, config.host)
        this._command = null;
        this._commands = []
        this._id = ""
        this._queue = ""
        this._consumer = undefined

        this.packetParser = new PacketParser((packet) => {
            this.handlePacket(packet)
        })

        this.stream.on("connect", (c) => {
       

            this.stream.setKeepAlive(true);

            this.stream.setNoDelay(true);

        })


        this.stream.on("data", (data) => {
            // console.log(data, "DATA")
            this.packetParser.execute(data)

        })

        this.addCommand({ type: "connect", args: "" })
    }

    handlePacket(packet) {
        if (packet) {
            if (packet.type == "heartbeat") {
                try {
                    const HEARTBEAT = Buffer.alloc(1);
                    HEARTBEAT.writeInt8(-1); // -1 as a heartbeat sig
                    this.stream.write(HEARTBEAT)
                } catch (error) {
                    // connection problem?
                }

                return
            }

            if(packet.type == "msg"){

                    // console.log("m", packet)
                    this._consumer(packet.msg)
                    // this.nextCmd()
                  
                return
            }
            // type of command
            if(!this._command) return;
            switch (this._command.type) {
                case "connect":
                    this._id = packet.id
                    console.log(packet.id)
                    this.nextCmd()
                    break;
                case "QueueDeclare":
                    console.log("QueueDeclare")
                    if (packet.res == SUCCESS || packet.res == QUEUALREADYEXIST) {
                        this._command.cb(packet.res)
                        this.nextCmd()
                    }
                 break;
                case "Publish":
                    if(packet.res == ERROR){
                        this._command.cb(new Error(`Couldnt put message wara ${this._command.payload}`))
                    }else{
                        this._command.cb(`success :${packet.res} message: ${this._command.payload}`)
                    }
                    this.nextCmd()
                    break;
                case "Ack":
                    if(packet.res == SUCCESS){
                        this._command.cb(true)
                    }else{
                        this._command.cb(false)

                    }
                    this.nextCmd()
                    break;
                case "Consume":
                    this.nextCmd()
                    break;
                default:
                    this.nextCmd()
                    break;
            }
        }
    }

    nextCmd() {
        this._command = this._commands.shift()
        if(!this._command) return;
        console.log(this._command, "current command");
        this.executeCmd()
    }
    writePacket() {

    }

    /**
     * 
     * @param { {name: string,  config: {QueueExpiry: number,MessageExpiry: number ,AckExpiry: number,Durable: boolean,noAck: boolean}}} opts 
     * @param {()=> void}
     */
    QueueDeclare(opts, cb) {
        if(this._command  && this._command.type != "QueueDeclare"){
            console.log(this._command.type)
          this.addCommand({type: "QueueDeclare", opts, cb})
          return;
        }

        if(!this._command){
          this.addCommand({type: "QueueDeclare", opts, cb})
        }
        
        this._queue = opts.name
        const queuename = Buffer.from(this._queue, "utf-8")
        const opts_ = Buffer.from(JSON.stringify(opts.config == undefined ? {} : opts.config), "utf-8")
        const op = Buffer.alloc(6);
        op.writeInt8(NEWQUEUE, 0); // Send a "health check" opcode
        op.writeInt8(opts.config == undefined ? NOOPTS : JSONENCODED, 1);
        op.writeUint32BE(queuename.length, 2)
        const combinedBuffer = Buffer.concat([op, queuename, opts_])
        this.stream.write(combinedBuffer)
     
    }

    /**
     * 
     * @param {string} payload 
     * @param {() => void} cb 
     * @returns 
     */
    Publish(payload, cb) {
        if(this._command && this._command.type != "Publish"){
            this.addCommand({type: "Publish", payload, cb})
            return;
          }

        if(!this._command){
            this.addCommand({type: "Publish", payload, cb})

        }
        // console.log('Client connected.');
        const data = Buffer.from(payload, "utf-8")
        const meta = Buffer.from(JSON.stringify({ queue:  this._queue }), "utf-8")

        const op = Buffer.alloc(6);

        op.writeInt8(PUBLISH, 0);
        op.writeInt8(JSONENCODED, 1);
        op.writeUint32BE(data.length, 2)

        const combinedBuffer = Buffer.concat([op, data, meta])
        console.log(combinedBuffer, "combined")
        this.stream.write(combinedBuffer);
    }

    Consume(queuname, cb) {
        if(this._command && this._command.type != "Consume"){
            this.addCommand({type: "Consume", queuname, cb})
            return;
          }

        if(!this._command){
            this.addCommand({type: "Consume", queuname, cb})

        }
        const data = Buffer.from(this._id, "utf-8")
        const meta = Buffer.from(queuname, "utf-8")
        
        const op = Buffer.alloc(6);

        op.writeInt8(SUBSCRIBE, 0);
        op.writeInt8(NOOPTS, 1);
        op.writeUint32BE(data.length, 2)
        const combinedBuffer = Buffer.concat([op, data, meta])
        // console.log(combinedBuffer, "combined")
        this.stream.write(combinedBuffer);
    }


    Ack(cb){
        if(this._command && this._command.type != "Ack"){
            this.addCommand({type: "Ack", cb})
            return;
          }

        if(!this._command){
            this.addCommand({type: "Ack",cb})

        }

        const data = Buffer.from(this._id, "utf-8")
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

        if (!this._command) {
            this._command = cmd;
            this.executeCmd()

        }
        else {

            this._commands.push(cmd);
        }
        return cmd;

    }


    executeCmd() {
     
       console.log("exec", this._command)
        if (this._command.type == "QueueDeclare") {
            this.QueueDeclare(this._command.opts, this._command.cb)
            return
        }

        if(this._command.type == 'Publish'){
            this.Publish(this._command.payload, this._command.cb)
            return
        }

        if(this._command.type == "Consume"){
            this.Consume(this._command.queuname, this._command.cb)
            this._consumer = this._command.cb
            console.log(this._consumer)
            // this.nextCmd()
            return
        }
    }
}