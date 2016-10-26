<?php

namespace PhpScotland2016\Demo\Service\Impl\Rmq;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Connection\AMQPStreamConnection;

use PhpScotland2016\Demo\Service\Interfaces\DemoServiceRequest;
use PhpScotland2016\Demo\Service\Interfaces\DemoServiceResponse;
use PhpScotland2016\Demo\Service\Interfaces\DemoServiceInterface;

class DemoServiceRmqProducer implements DemoServiceInterface
{
	protected $_amqp_conn = null;
	protected $_amqp_chan = null;
	protected $_run = true;

	public function __construct() {
		$this->connect();
	}

	public function __destruct() {
		if(!is_null($this->_amqp_chan)) $this->_amqp_chan->close();
		if(!is_null($this->_amqp_conn)) $this->_amqp_conn->close();
	}

	public function handleRequest(DemoServiceRequest $request) {
		$msg = new AMQPMessage($request->get());
		$this->_amqp_chan->basic_publish($msg, "", $_ENV["RMQ_QUEUE"]);
		$message = $request->getAsArray();
		$message['result'] = 0;
		$message['msg'] = 'See websocket response';
		return new DemoServiceResponse($message);
	}

	protected function connect() {
		try {
			$rmq_ready = false;
			while(!$rmq_ready) {
				$this->_amqp_conn = new AMQPStreamConnection(
					$_ENV["RMQ_HOST"],
					$_ENV["RMQ_PORT"],
					$_ENV["RMQ_USER"],
					$_ENV["RMQ_PASS"]
				);
				$rmq_ready = true;
			}
			
		}
		catch(\Exception $e) {
			$this->log("RabbitMQ server not ready for us, sleeping...");
			sleep(3);
		}
		$this->_amqp_chan = $this->_amqp_conn->channel();
		$this->_amqp_chan->queue_declare($_ENV["RMQ_QUEUE"],
			false, // passive
			false, // durable
			false, // exclusive
			true   // auto_delete
		);
	}
}

