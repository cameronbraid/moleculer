/*
 * moleculer
 * Copyright (c) 2018 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

"use strict";

const Promise				= require("bluebird");
const { MoleculerError } 	= require("../errors");
const Transporter 			= require("./base");

/**
 * Transporter for Redis
 *
 * @class RedisTransporter
 * @extends {Transporter}
 */
class RedisSentinelTransporter extends Transporter {


	/**
	 * Creates an instance of RedisTransporter.
	 *
	 * @param {any} opts
	 *
	 * @memberof RedisTransporter
	 */
	constructor(opts) {
		super(opts);

		try {
			this.Redis = require("ioredis");
		} catch(err) {
			/* istanbul ignore next */
			this.broker.fatal("The 'ioredis' package is missing. Please install it with 'npm install ioredis --save' command.", err, true);
		}

		if (!(opts.sentinels && opts.sentinels.length)) {
			this.broker.fatal("At least one sentinel needs to be spceified in opts.sentinels" );
		}
		if (!opts.name) {
			this.broker.fatal("The master name needs to be spceified in opts.name" );
		}

	}

	/**
	 * Connect to the server
	 *
	 * @memberof RedisTransporter
	 */
	connect() {
		return new Promise((resolve, reject) => {

			this.clientSub = new this.Redis(this.opts);
			this.clientPub = new this.Redis(this.opts);
			this.clientSub.on("messageBuffer", (topicBuf, buf) => {
				const topic = topicBuf.toString();
				//console.log("rec " + topic)
				const cmd = topic.split(".")[1];
				
				this.incomingMessage(cmd, buf);
			});
			this.onConnected().then(resolve);
		});
	}

	/**
	 * Disconnect from the server
	 *
	 * @memberof RedisTransporter
	 */
	disconnect() {
		if (this.clientSub) {
			this.clientSub.disconnect();
			this.clientSub = null;
		}

		if (this.clientPub) {
			this.clientPub.disconnect();
			this.clientPub = null;
		}
	}

	/**
	 * Subscribe to a command
	 *
	 * @param {String} cmd
	 * @param {String} nodeID
	 *
	 * @memberof RedisTransporter
	 */
	subscribe(cmd, nodeID) {
		//console.log("sub " + this.getTopicName(cmd, nodeID))
		this.clientSub.subscribe(this.getTopicName(cmd, nodeID));
		return Promise.resolve();
	}

	/**
	 * Publish a packet
	 *
	 * @param {Packet} packet
	 *
	 * @memberof RedisTransporter
	 */
	publish(packet) {
		/* istanbul ignore next*/
		if (!this.clientPub) return Promise.reject(new MoleculerError("Redis Client is not available"));

		const data = this.serialize(packet);
		this.incStatSent(data.length);
		//console.log("pub " + this.getTopicName(packet.type, packet.target), packet)
		this.clientPub.publish(this.getTopicName(packet.type, packet.target), data);
		return Promise.resolve();
	}

}

module.exports = RedisSentinelTransporter;
