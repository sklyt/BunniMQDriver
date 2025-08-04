export default class PacketParser {
    constructor(callback) {
        this.callback = callback;
    }

    /**
     * @param {Buffer} packet
     */
    execute(packet) {
        const opCode = packet.readInt8(0);
        const parsedPacket = {};

        switch (opCode) {
            case -1:
                this._handleHeartbeat(parsedPacket);
                break;

            case -7:
                this._handleHandshake(packet, parsedPacket);
                break;

            case -8:
                this._handleAuthentication(packet, parsedPacket);
                break;

            case 126:
            case 127:
                this._handleErrorSuccess(opCode, parsedPacket);
                break;

            case 1:
                this._handleMessage(packet, parsedPacket);
                break;

            default:
                // Handle unknown opCode if needed
                break;
        }
    }

    _handleHeartbeat(parsedPacket) {
        parsedPacket.type = "heartbeat";
        this.callback(parsedPacket);
    }

    _handleHandshake(packet, parsedPacket) {
        parsedPacket.type = "HANDSHAKE";
        parsedPacket.id = packet.subarray(1).toString();
        this.callback(parsedPacket);
    }

    _handleAuthentication(packet, parsedPacket) {
        const flag = packet.readInt8(1);
        parsedPacket.type = "AUTHENTICATED";

        if (flag === 1) {
            parsedPacket.status = 200;
        } else {
            const errorLength = packet.readInt32BE(2);
            const errorMessage = packet.subarray(6, 6 + errorLength);
            parsedPacket.status = 404;
            parsedPacket.errorMsg = errorMessage;
        }

        this.callback(parsedPacket);
    }

    _handleErrorSuccess(opCode, parsedPacket) {
        parsedPacket.type = "error|success";
        parsedPacket.res = opCode;
        this.callback(parsedPacket);
    }

    _handleMessage(packet, parsedPacket) {
        const messageLength = packet.readUint32BE(2);
        parsedPacket.type = "msg";
        parsedPacket.msg = packet.subarray(6, 6 + messageLength).toString();
        this.callback(parsedPacket);
    }
}