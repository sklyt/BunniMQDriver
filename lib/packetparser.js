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
        // console.log("connection")
        p.type = "HANDSHAKE"
        p.id = packet.subarray(1).toString()
        this.cb(p)
        return
      }

      if(op == -8){
        const flag = packet.readInt8(1)
        p.type = "AUTHENTICATED"
        if(flag == 1){
        
          p.status = 200
          
        }else{
         const errolen = packet.readInt32BE(2)
         const errormsg = packet.subarray(6, 6 + errolen)
         p.status = 404
         p.errorMsg = errormsg
        }

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