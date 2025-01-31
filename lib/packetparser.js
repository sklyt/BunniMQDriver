export default class PacketParser{

    constructor(cb){
     this.cb = cb
    }
 /**
  * 
  * @param {Buffer} packet 
  */
    execute(packet){
      const op = packet.readInt8(0)
      // console.log("op", op)
      let p = {}
      if(op == -1){
        p.type = "heartbeat"
        this.cb(p)
        return
      }

      if(op == -7){
        console.log("connection")
        p.type = "connect"
        p.id = packet.subarray(1).toString()
        this.cb(p)
        return
      }

      if(op == 126 || op == 127){
        p.type = "error|success"
        p.res = op
        this.cb(p)
        return
      }

      if(op == 1){
        const msglen = packet.readUint32BE(2)
        p.type = "msg"
        p.msg = packet.subarray(6, 6+msglen).toString()
        // console.log("message", p)
        this.cb(p)
        
        return
        
      }
    }
}